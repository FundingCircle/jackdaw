(ns jackdaw.test.fixtures
  "Test fixtures for kafka based apps"
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [jackdaw.client :as jc]
            [jackdaw.client.extras :as jce]
            [jackdaw.test.config :as config]
            [jackdaw.test.kafka :as broker]
            [jackdaw.test.kc :as kc]
            [jackdaw.test.fs :as fs]
            [jackdaw.test.zk :as zk])
  (:import [io.confluent.kafka.schemaregistry.rest SchemaRegistryConfig SchemaRegistryRestApplication]))

;; services

(defn zookeeper
  "A zookeeper test fixture

   Start up a zookeeper broker with the supplied config before running
   the test `t`"
  [config]
  (fn [t]
    (let [snapshot-dir (fs/tmp-dir "zookeeper-snapshot")
          log-dir  (fs/tmp-dir "zookeeper-log")
          _ (fs/delete-directories! snapshot-dir log-dir)
          zk (zk/start! {:config       config
                         :snapshot-dir snapshot-dir
                         :log-dir      log-dir})]
      (try
        (log/info "Started zookeeper fixture" zk)
        (t)
        (finally
          (log/info "Stopping zookeeper")
          (zk/stop! zk)
          (log/info "Stopped zookeeper fixture" zk))))))

(defn broker
  "A kafka test fixture.

   Start up a kafka broker with the supplied config before running the

   test `t`.

   If the optional `num-brokers` is provided, start up a cluster with that
   many brokers. Unfortunately there is a bit of a teardown cost to this
   as when you shutdown a broker, kafka tries to shuffle all it's data
   across to any remaining live brokers so use this with care. We've found
   that you don't really need this unless you're trying to test some weird
   edge case.

   Note that when using num-brokers > 1, you must explicitly set port and
   it must agree with the port implicit in `listeners` and/or
   `advertised.listeners`."
  ([config num-brokers]
   (fn [t]
     (let [multi-config (config/multi-config config)
           configs (if (= 1 num-brokers)
                     ;; no need to rewrite the config if we just have a single
                     ;; broker and the multi-config stuff can cause problems
                     ;; if you don't specify a port
                     [config]
                     (map multi-config (range num-brokers)))
           cluster (doall (map (fn [cfg]
                                 (fs/delete-directories! (get cfg "log.dirs"))
                                 (broker/start! {:config cfg}))
                               configs))]
       (try
         (log/info "Started " (if (> num-brokers 1) "multi" "single") "broker fixture" cluster)
         (t)
         (finally
           (log/info "Stopping kafka")
           ;; This takes a surprisingly
           (doseq [node cluster]
             (broker/stop! node))
           (log/info "Stopped multi-broker fixture" cluster))))))
  ([config]
   (broker config 1)))

(defn multi-broker
  "DEPRECATED: prefer use `broker` with the optional `num-brokers` argument"
  [config n]
  (broker config n))

(defn schema-registry
  [config]
  (fn [t]
    (let [app (SchemaRegistryRestApplication.
               (SchemaRegistryConfig. (jc/map->properties config)))
          server (.createServer app)]
      (try
        (.start server)
        (log/info "Started schema registry fixture" server)
        (t)
        (finally
          (.stop server)
          (log/info "Stopped schema registry fixture" server))))))

(defn kafka-connect
  [worker-config]
  (fn [t]

    (let [kc-runner (kc/start! {:config worker-config})]
      (try
        (log/info "Started Kafka Connector in standalone mode")
        (t)

        (finally
          (log/info "Shutting down kafka connect worker")
          (kc/stop! kc-runner))))))

;; fixture composition

(defn identity-fixture
  "They have this already in clojure.test but it is called
   `default-fixture` and it is private. Probably stu seirra's fault
   :troll:"
  [t]
  (t))

;;


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

(defn soft-tracking-id
  [v]
  (or (get-in v [:metadata :tracking-id])
      (:tracking-id v)))

(defn records-for-tracking-ids [tracking-ids]
  (fn [[_ x]]
    (contains? tracking-ids (soft-tracking-id x))))

(defn records-for-tracking-id [tracking-id]
  (records-for-tracking-ids #{tracking-id}))

(defn records-for-ids [ids]
  (fn [[_ x]]
    (contains? ids (:id x))))

(defn records-for-id [id]
  (records-for-ids #{id}))
