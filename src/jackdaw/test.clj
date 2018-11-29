(ns jackdaw.test
  ""
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.spec.alpha :as s]
            [jackdaw.client :as jc]
            [jackdaw.client.partitioning :as jcp]
            [jackdaw.client.log :as jcl]
            [jackdaw.data :as jd])
  (:import org.apache.kafka.clients.consumer.ConsumerRecord))

(defn fuse
  "Returns a function that throws an exception when called after some
  time has passed."
  [millis]
  (let [end (+ millis (System/currentTimeMillis))]
    (fn [_]
      (if (< end (System/currentTimeMillis))
        (throw (ex-info "Timer expired" {:millis millis}))
        true))))

(defn keep-topic
  "Given a topic and a stream (seq) of `ConsumerRecords` read from
  multiple topics, select only those records in the stream which are
  from the given topic.

  Return the selected `ConsumerRecord` instances.

  See `#'jackdaw.data/ConsumerRecord->data` for a way to unpack
  `ConsumerRecord` instances."
  [{:keys [:topic-name]} messages]
  (->> messages
       (filter #(= topic-name (:topic-name %)))))

(defn validate-value!
  "Throws an exception if the topic has a spec, but the value does not
  satisify it. Otherwise flows the value."
  [{:keys [:value-spec :topic-name] :as topic} v]
  (when value-spec
    (if-let [error (s/explain-data value-spec v)]
      (throw (ex-info (str "Message on " topic-name " is invalid")
                      (s/explain-data value-spec v)))
      v)))

(defn produce-all-inputs!
  ""
  [bootstrap-servers inputs]
  (doseq [input inputs]
    (if (fn? input)
      ;; Input is function, just invoke
      (input)

      ;; Input is map of describing what to write to Kafka
      (let [{:keys [topic record-values post-sleep-ms validate?]
             :or {validate? true}} input]

        (with-open [p (jc/producer {"bootstrap.servers" bootstrap-servers} topic)]
          (doseq [value record-values]
            (-> value
                (cond->> validate? (validate-value! topic))
                (->> (jcp/->ProducerRecord p topic)
                     (jc/send! p))
                deref)))

        (when post-sleep-ms
          (Thread/sleep post-sleep-ms))))))

(defn consume-all-outputs!
  ""
  [consumer outputs]
  (let [records (jcl/seq-until consumer 100 (+ (System/currentTimeMillis) 10000))]

    (doall (for [{n :take :keys [topic pred]} outputs
                 :let [{:keys [:topic-name]} topic
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

               (doall topic-records))))))

(s/def ::topic map?)
(s/def ::post-sleep-ms integer?)
(s/def ::record-values sequential?)
(s/def ::validate? boolean?)

(s/def ::input
  (s/or ::producer (s/keys :req-un [::topic ::record-values]
                           :opt-un [::post-sleep-ms ::validate?])
        ::executor fn?))

(s/def ::inputs
  (s/coll-of ::input))

(s/def ::pred fn?)
(s/def ::take integer?)
(s/def ::output
  (s/keys :req-un [::topic ::pred]
          :opt-un [::take]))

(s/def ::outputs
  (s/coll-of ::output))

(defn produce-timed-results
  "Produces input messages to Kafka and returns a list of each of the messages produced to each of the output topics.

  Each input can have the following keys:
  * topic: Topic to write messages to. Topic can have the following optional keys:
    * :jackdaw/key-fn: Function that returns the record key
    * :jackdaw/partition-fn: Function to determine which partition to publish the message on
    * :jackdaw/value-spec: Spec to validate messages before writing to Kafka
  * record-values: List of messages to write to Kafka
  * post-sleep-ms: Milliseconds to sleep after writing to Kafka. Useful if you want messages to be processed before continuing
  * validate?: True if the values should be validated before producing, false otherwise. Defaults to `true`. Use to deliberately produce bad data.

  Each output can have the following keys:
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
    (jc/assign-all consumer (map :topic outputs))
    (jc/seek-to-end-eager consumer)
    ;; Force realization of offsets before producing
    (jc/poll consumer 0)
    (produce-all-inputs! bootstrap-servers inputs)
    (consume-all-outputs! consumer outputs)))
