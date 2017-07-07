(ns jackdaw.client-test
  (:require [clojure.test :refer :all]
            [jackdaw.client :as client])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord ConsumerRecords]
           [org.apache.kafka.clients.producer Producer]
           org.apache.kafka.common.TopicPartition))

(def producer-config
  {"bootstrap.servers" "localhost:9092"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})

(def consumer-config
  {"bootstrap.servers"     "localhost:9092"
   "group.id"              "test"
   "key.deserializer"      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"    "org.apache.kafka.common.serialization.StringDeserializer"
   "metadata.max.age.ms"   "1000" ;; usually this is 5 minutes
   "auto.offset.reset"     "earliest"
   "enable.auto.commit"    "true"})

(deftest callback-test
  (testing "producer callbacks"
    (testing "success"
      (let [result (promise)
            cb (client/callback (fn [meta ex]
                                  (if ex
                                    (deliver result ex)
                                    (deliver result :ok))))]

        (.onCompletion cb nil nil)
        (is (= :ok @result ))))

    (testing "failure"
      (let [result (promise)
            cb (client/callback (fn [meta ex]
                                  (if ex
                                    (deliver result ex)
                                    (deliver result :ok))))
            ex (Exception. "failed write :-(")]
        (.onCompletion cb nil ex)
        (is (= ex @result))))))

(defn poll-result [topic data]
  (let [partition 1
        offset 1]
    (ConsumerRecords.
     {(TopicPartition. topic partition)
      (map (fn [[k v]]
             (ConsumerRecord. topic partition offset k v)) data)})))

(defn mock-consumer
  "Returns a consumer that will return the supplied items (as ConsumerRecords)
   in response to successive calls of the `poll` method"
  [queue]
  (reify Consumer
    (poll [this ms]
      (.poll queue ms TimeUnit/MILLISECONDS))))

(deftest consumer-test
  (let [config {"bootstrap.servers" "localhost:9092"
                "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}]
    (is (instance? Consumer (client/consumer config)))))

(deftest producer-test
  (let [config {"bootstrap.servers" "localhost:9092"
                "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}]
    (is (instance? Producer (client/producer config)))))

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
