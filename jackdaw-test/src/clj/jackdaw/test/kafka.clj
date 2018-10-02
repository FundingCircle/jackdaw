(ns jackdaw.test.kafka
  (:require
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.string :as string]
   [jackdaw.client :as jc]
   [jackdaw.client.extras :as jce]
   [jackdaw.test.fs :as fs])
  (:import
   (kafka.server BrokerMetadata
                 KafkaConfig
                 BrokerMetadataCheckpoint
                 KafkaServerStartable)))

(defn start!
  "Starts a kakfa broker.

   Returns a map containing the broker instance itself and a latch
   that waits until the broker is shutdown"
  [{:keys [config]}]
  (let [props           (java.util.Properties.)
        checkpoint-file (io/file (get config "log.dirs") "meta.properties")
        broker-id       (Integer/parseInt (get config "broker.id"))]
    ;; Create the configured log directory
    (io/make-parents checkpoint-file)

    ;; Kafka expects a meta.properties file to exist in the configured
    ;; log directory. The meta.properties structure is a serialized
    ;; BrokerMetadataCheckpoint object, so just instantiate one and
    ;; use its own writing machinery.
    (. (BrokerMetadataCheckpoint. checkpoint-file)
       (write (BrokerMetadata. broker-id)))

    ;; Generate the KafkaConfig (java.util.Properties) map that we
    ;; need to start the broker
    (.putAll props config)
    (let [broker (-> props
                     (KafkaConfig.)
                     (KafkaServerStartable.))]
      (.startup broker)
      {:broker broker
       :config config})))

(defn stop!
  "Stops a kafka broker.

   Shuts down the broker, releases the latch, and deletes log files"
  [{:keys [broker config log-dirs]}]
  (when broker
    (try
      (.shutdown broker)
      (.awaitShutdown broker)
      {:broker nil}
      (finally
        (fs/try-delete! (-> config (get "log.dirs") (io/file)))))))

(defn fuse
  "Returns a function that throws an exception when called after some time has
  passed."
  [millis]
  (let [end (+ millis (System/currentTimeMillis))]
    (fn [_]
      (if (< end (System/currentTimeMillis))
        (throw (ex-info "Timer expired" {:millis millis}))
        true))))

(defn matching-topic [expected-topic]
  (fn [message]
    (= (:topic message)
       (:topic.metadata/name expected-topic))))

(defn keep-topic [expected-topic messages]
  (->> messages
       (filter (matching-topic expected-topic))
       (map jce/->message)))

(defn validate-value!
  "Throws an exception if the topic has a spec specified but the value does not satisify it."
  [{:keys [value-spec] :as topic} [_ x]]
  (when (and value-spec
             (not (s/valid? value-spec x)))
    (throw (ex-info (str "Message on " (:jackdaw.topic/topic-name topic) " is invalid")
                    (s/explain-data value-spec x)))))

(s/def ::topic map?)
(s/def ::post-sleep-ms integer?)
(s/def ::record-values sequential?)
(s/def ::validate? boolean?)
(s/def ::input (s/or ::producer (s/keys :req-un [::topic ::record-values]
                                        :opt-un [::post-sleep-ms ::validate?])
                     ::executor fn?))
(s/def ::inputs (s/coll-of ::input))

(s/def ::pred fn?)
(s/def ::take integer?)
(s/def ::output (s/keys :req-un [::topic ::pred]
                        :opt-un [::take]))
(s/def ::outputs (s/coll-of ::output))

(defn produce-all-inputs! [bootstrap-servers inputs]
  (doseq [input inputs]
    (if (fn? input)
      ;; Input is function, just invoke
      (input)

      ;; Input is map of describing what to write to Kafka
      (let [{:keys [topic record-values post-sleep-ms validate?] :or {validate? true}} input
            key-fn (get topic :key-fn (constantly nil))
            partition-fn (get topic :partition-fn (constantly nil))
            value-spec (:value-spec topic)
            topic-name (:jackdaw.topic/topic-name topic)]

        (with-open [p (jc/producer {"boostrap.servers" bootstrap-servers} topic)]
          (doseq [value record-values]

            (when (and validate? value-spec (not (s/valid? value-spec value)))
              (throw (ex-info (str "Attempted to publish invalid record to " topic-name " " (s/explain-data value-spec value))
                              (s/explain-data value-spec value))))

            (jc/send! p (jc/producer-record topic
                                            (partition-fn value)
                                            (key-fn value)
                                            value))))

        (when post-sleep-ms
          (Thread/sleep post-sleep-ms))))))

(defn produce-timed-results
  "Produces input messages to Kafka and returns a list of each of the messages produced to each of the output topics.

  inputs: List of message groups to produce to Kafka, or can be an arbitrary function. Each input can have the following keys:
  * topic: Topic to write messages to. Topic can have the following optional keys:
    * key-fn: Function that returns the record key
    * partition-fn: Function to determine which partition to publish the message on
    * value-spec: Spec to validate messages before writing to Kafka
  * record-values: List of messages to write to Kafka
  * post-sleep-ms: Milliseconds to sleep after writing to Kafka. Useful if you want messages to be processed before continuing
  * validate?: True if the values should be validated before producing, false otherwise. Defaults to `true`. Use to deliberately produce bad data.

  outputs: List of topics that will be produced to as a result of publishing the inputs. Each output can have the following keys:
  * topic: Topic that will be consumed from. Topic can have the following optional keys:
    * value-spec: Spec that all returned messages must satisify
  * pred: Tests whether a record should be included in the results
  * take: Number of records expected. If n is present and < n records are found, an exception is thrown. If n is not present, all records produced in the timeout period will be returned

  return value: List of lists that correspond to the outputs given. If n outputs are specifed, the returned list will have n lists in the same order as the outputs.
  Example:
  ```
  [topic-a-records topic-b-records]
  (produce-timed-results [{:topic (topics/input-topic) :messages [1 2]}]
                         [{:topic (topics/output-topic-a) :pred-fn (constantly true) :take 1}
                          {:topic (topics/output-topic-b) :pred-fn (constantly true)} :take 2])
  ```
  "
  [bootstrap-servers inputs outputs]

  {:pre [(s/valid? ::inputs inputs)
         (s/valid? ::outputs outputs)]}

  (with-open [consumer (jc/consumer {"bootstrap.servers" bootstrap-servers
                                     "auto.offset.reset" "latest"
                                     "enable.auto.commit" "false"}
                                    (:topic (first outputs)))]

    (apply jce/assign-all consumer (map :topic outputs))

    (jc/seek-to-end-eager consumer)

    ;; Force realization of offsets before producing
    (jc/poll consumer 0)

    (produce-all-inputs! bootstrap-servers inputs)

    (let [records (jce/log-seq-until consumer 100 (+ (System/currentTimeMillis) 10000))]

      (doall (for [{n :take :keys [topic pred]} outputs
                   :let [topic-name (:jackdaw.topic/topic-name topic)
                         topic-records (->> records
                                            (keep-topic topic)
                                            (filter pred)
                                            (map (fn [x] (validate-value! topic x) x)))]]
               (if n
                 (let [actual-records (doall (take n topic-records))]
                   (when (not= n (count actual-records))
                     (throw (ex-info (str "Expected " n " records on " topic-name " matching predicate, but found " (count actual-records))
                                     {:records actual-records
                                      :n n})))
                   actual-records)

                 (doall topic-records)))))))
