(ns jackdaw.client-test
  (:require [clojure.test :refer [are deftest is testing]]
            [jackdaw.admin :as admin]
            [jackdaw.client :as client]
            [jackdaw.test.fixtures :as fix]
            [jackdaw.test.serde :as serde]
            [jackdaw.data :as data])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]
           java.time.Duration
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord ConsumerRecords]
           [org.apache.kafka.clients.producer Producer]
           org.apache.kafka.common.TopicPartition))

(set! *warn-on-reflection* false)

(def foo-topic
  (serde/resolver {:topic-name "foo"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :string
                   :value-serde :json}))

(def bar-topic
  (serde/resolver {:topic-name "bar"
                   :replication-factor 1
                   :partition-count 1
                   :key-serde :string
                   :value-serde :json}))

(def high-partition-topic
  (serde/resolver {:topic-name "high-partition-topic"
                   :replication-factor 1
                   :partition-count 15
                   :key-serde :string
                   :value-serde :json}))

(def test-topics
  {"foo" foo-topic
   "bar" bar-topic
   "high-partition-topic" high-partition-topic})

(defn broker-config []
  {"bootstrap.servers" "localhost:9092"})

(defn producer-config []
  (-> (broker-config)
      (merge {"key.serializer" (.getName org.apache.kafka.common.serialization.StringSerializer)
              "value.serializer" (.getName org.apache.kafka.common.serialization.StringSerializer)})))

(defn consumer-config [group-id]
  (-> (broker-config)
      (merge {"group.id" group-id
              "key.deserializer" (.getName org.apache.kafka.common.serialization.StringDeserializer)
              "value.deserializer" (.getName org.apache.kafka.common.serialization.StringDeserializer)})))

;; Producer API
;;

(defn with-producer [producer f]
  (with-open [p producer]
    (f p)))

(def +response-keys+
  {:send! [:topic-name :partition  :offset
           :timestamp
           :serialized-key-size :serialized-value-size]

   :produce! [:topic-name :partition  :offset
              :timestamp
              :serialized-key-size :serialized-value-size]

   :partitions-for [:topic-name :isr :leader :replicas :partition :offline-replicas]})

(defn response-ok? [response-for x]
  (every? #(contains? x %) (get +response-keys+ response-for)))

(defn callback-ok? [x]
  (= :ok x))

(deftest producer-test
  (with-producer (client/producer (producer-config))
    (fn [producer]
      (is (instance? Producer producer)))))

(deftest callback-test
  (testing "producer callbacks"
    (testing "success"
      (let [result (promise)
            cb (client/callback (fn [meta ex]
                                  (if ex
                                    (deliver result ex)
                                    (deliver result :ok))))]

        (.onCompletion cb nil nil)
        (is (= :ok @result))))

    (testing "failure"
      (let [result (promise)
            cb (client/callback (fn [meta ex]
                                  (if ex
                                    (deliver result ex)
                                    (deliver result :ok))))
            ex (Exception. "failed write :-(")]
        (.onCompletion cb nil ex)
        (is (= ex @result))))))


(deftest ^:integration send!-test
  (fix/with-fixtures [(fix/topic-fixture (broker-config) test-topics 1000)]
    (with-producer (client/producer (producer-config))
      (fn [producer]
        (testing "simple send"
          (let [msg (data/->ProducerRecord {:topic-name "foo"} "1" "one")
                result (client/send! producer msg)]
            (is (response-ok? :send! @result))))

        (testing "send with callback"
          (let [msg (data/->ProducerRecord {:topic-name "foo"} "1" "one")
                on-callback (promise)
                result (client/send! producer msg (fn [meta ex]
                                                    (if ex
                                                      (deliver on-callback ex)
                                                      (deliver on-callback :ok))))]
            (is (response-ok? :send! @result))
            (is (callback-ok? @on-callback))))))))

(deftest ^:integration produce!-test
  (fix/with-fixtures [(fix/topic-fixture (broker-config) test-topics 1000)]
    (with-producer (client/producer (producer-config))
      (fn [producer]
        (let [{:keys [topic key value partition timestamp headers]}
              {:topic     foo-topic
               :key       "1"
               :value     "one"
               :partition 0
               :timestamp (System/currentTimeMillis)
               :headers {}}]

          (testing "topic, value"
            (is (response-ok? :produce! @(client/produce! producer topic value))))

          (testing "topic, key, value"
            (is (response-ok? :produce! @(client/produce! producer topic key value))))

          (testing "topic, partition, key, value"
            (is (response-ok? :produce! @(client/produce! producer topic partition key value))))

          (testing "topic, partition, timestamp, key, value"
            (is (response-ok? :produce! @(client/produce! producer topic partition timestamp key value))))

          (testing "topic, partition, timestamp, key, value, headers"
            (is (response-ok? :produce @(client/produce! producer topic partition timestamp key value headers)))))))))

;; Consumer API

(defn with-consumer [consumer f]
  (with-open [c consumer]
    (f c)))

(deftest ^:integration consumer-test
  (let [config {"group.id" "jackdaw-client-test-consumer-test"
                "bootstrap.servers" "localhost:9092"
                "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}
        key-serde (:key-serde foo-topic)
        value-serde (:value-serde foo-topic)]

    (testing "create with config"
      (with-consumer (client/consumer config)
        (fn [consumer]
          (is (instance? Consumer consumer)))))


    (testing "create with config and serdes"
      (with-consumer (client/consumer config {:key-serde key-serde
                                              :value-serde value-serde})
        (fn [consumer]
          (is (instance? Consumer consumer)))))

    (testing "topic subscription"
      (with-consumer (-> (client/consumer config)
                         (client/subscribe [foo-topic]))
        (fn [consumer]
          (is (instance? Consumer consumer))
          (is (= #{"foo"}
                 (client/subscription consumer))))))

    (testing "topic assignment"
      (with-consumer (-> (client/consumer config)
                         (client/assign-all (map :topic-name [foo-topic])))
        (fn [consumer]
          (is (instance? Consumer consumer))
          (is (= [{:topic-name "foo" :partition 0}]
                 (client/assignment consumer))))))))

(deftest ^:integration partitions-for-test
  (fix/with-fixtures [(fix/topic-fixture (broker-config) test-topics 1000)]
    (let [key-serde (:key-serde high-partition-topic)
          value-serde (:value-serde high-partition-topic)]

      (testing "partition info"
        (with-consumer (-> (client/consumer (consumer-config "partition-test"))
                           (client/subscribe [bar-topic]))
          (fn [consumer]
            (let [[pinfo] (-> (client/partitions-for consumer bar-topic)
                              (data/datafy))]
              (is (response-ok? :partitions-for pinfo))))))

      (testing "single-partition consumer"
        (with-consumer (-> (client/consumer (consumer-config "partition-test"))
                           (client/subscribe [bar-topic]))
          (fn [consumer]
            (is (= 1 (client/num-partitions consumer bar-topic))))))

      (testing "multi-partition consumer"
        (with-consumer (-> (client/consumer (consumer-config "partition-test"))
                           (client/subscribe [high-partition-topic]))
          (fn [consumer]
            (is (= 15 (client/num-partitions consumer high-partition-topic))))))

      (testing "single-partition producer"
        (with-producer (client/producer (producer-config))
          (fn [producer]
            (is (= 1 (client/num-partitions producer bar-topic))))))

      (testing "multi-partition producer"
        (with-producer (client/producer (producer-config))
          (fn [producer]
            (is (= 15 (client/num-partitions producer high-partition-topic)))))))))

(defn mock-consumer
  "Returns a consumer that will return the supplied items (as ConsumerRecords)
   in response to successive calls of the `poll` method"
  [queue]
  (reify Consumer
    (^ConsumerRecords poll [this ^long ms]
      (.poll queue ms TimeUnit/MILLISECONDS))
    (^ConsumerRecords poll [this ^Duration duration]
     (.poll queue (.toMillis duration) TimeUnit/MILLISECONDS))))

(defn poll-result [topic data]
  (let [partition 1
        offset 1]
    (ConsumerRecords.
     {(TopicPartition. topic partition)
      (map (fn [[k v]]
             (ConsumerRecord. topic partition offset k v)) data)})))

(deftest poll-test
  (let [q (LinkedBlockingQueue.)
        consumer (mock-consumer q)]
    (.put q (poll-result "test-topic" [[1 1] [2 2]]))
    (let [results (client/poll consumer 1000)]
      (are [k v] (first results)
           :topic "test-topic"
           :key 1
           :value 1)
      (are [k v] (second results)
           :topic "test-topic"
           :key 2
           :value 2))))

(deftest ^:integration position-all-test
  (fix/with-fixtures [(fix/topic-fixture (broker-config) test-topics 1000)]
    (let [key-serde (:key-serde high-partition-topic)
          value-serde (:value-serde high-partition-topic)]

      (with-consumer (-> (client/consumer (consumer-config "partition-test"))
                         (client/subscribe [bar-topic]))
        (fn [consumer]
          ;; without an initial `poll`, there is no position info
          (client/poll consumer 0)
          (is (= {{:topic-name "bar" :partition 0} 0}
                 (client/position-all consumer))))))))

(defn with-topic-data
  "Helper for creating a randomly named topic and seeding it with data
   obtained by invoking the supplied `data` function with the created
   topic config.

   The topic is deleted after invoking `f`"
  [data group-id f]
  (let [topic (str (java.util.UUID/randomUUID))
        topic-config {:topic-name topic
                      :partition-count 1
                      :replication-factor 1
                      :key-serde :string
                      :value-serde :string}]
    (with-open [admin (admin/->AdminClient (broker-config))]
      (admin/create-topics! admin [topic-config])

      (try
        (with-producer (client/producer (producer-config))
          (fn [producer]
            (doseq [record (data topic-config)]
              @(client/send! producer record))))

        (with-consumer (client/consumer (consumer-config group-id))
          (fn [consumer]
            (f consumer topic-config)))

        (finally
          (admin/delete-topics! admin [topic-config]))))))

(defn seek-test-data
  [topic]
  (->> [[1 "one"]
        [2 "two"]
        [3 "three"]
        [4 "four"]
        [5 "five"]
        [6 "six"]
        [7 "seven"]
        [8 "eight"]
        [9 "nine"]
        [10 "ten"]]
       (map (fn [[k v]]
              (let [timestamp k
                    partition 0]
                (data/->ProducerRecord topic partition timestamp (str k) v))))))

(deftest seek-test
  (testing "consumer with automatic subscription"
    (with-topic-data seek-test-data "seek-test"
      (fn [consumer topic-config]
        (testing "seek-to-end"
          (let [end-pos (-> (client/subscribe consumer [topic-config])
                            (client/seek-to-end-eager)
                            (client/position-all)
                            (vals)
                            first)]
            (is (= 10 end-pos))))

        (testing "seek-to-beginning"
          (let [begin-pos (-> (client/subscribe consumer [topic-config])
                              (client/seek-to-beginning-eager)
                              (client/position-all)
                              (vals)
                              first)]
            (is (= 0 begin-pos)))))))

  (testing "consumer with manual assignment"
    (with-topic-data seek-test-data "seek-test"
      (fn [consumer topic-config]
        (testing "seek to ts-next=10"
          (let [ts-next 10]
            (as-> consumer $
              (client/assign-all $ (map :topic-name [topic-config]))
              (client/seek-to-timestamp $ ts-next [topic-config])
              (client/position-all $)
              (is (= [(dec ts-next)]
                     (vals $))))))

        (testing "seek to ts-next=1"
          (let [ts-next 1]
            (as-> consumer $
              (client/assign-all $ (map :topic-name [topic-config]))
              (client/seek-to-timestamp $ ts-next [topic-config])
              (client/position-all $)
              (is (= [0]
                     (vals $))))))

        (testing "seek to ts-next=1000"
          (let [ts-next 1000]
            (as-> consumer $
              (client/assign-all $ (map :topic-name [topic-config]))
              (client/seek-to-timestamp $ ts-next [topic-config])
              (client/position-all $)
              (is (= [11]
                     (vals $))))))))))
