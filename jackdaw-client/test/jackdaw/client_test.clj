(ns jackdaw.client-test
  (:require [clojure.test :refer :all]
            [jackdaw.client :as client])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord ConsumerRecords]
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

(deftest select-methods-test
  (testing "object methods"
    (let [o (Object.)]
      (is (= {:getClass Object}
             (client/select-methods o [:getClass]))))))

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
  [topic queue]
  (reify Consumer
    (poll [this ms]
      (.poll queue ms TimeUnit/MILLISECONDS))))

(deftest log-messages-test
  (let [q (LinkedBlockingQueue.)
        consumer (mock-consumer "foo" q)
        live? (atom true)
        done? (fn []
                @live?)
        log (client/log-messages consumer 1000 done?)]

    (let [result (poll-result "foo" [[1 1]
                                     [2 2]])]

      (testing "can fetch items delivered to a topic"
        (.put q result)
        (let [[a b] (take 2 log)]
          (is (= [1 1] a))
          (is (= [2 2] b))))

      (testing "doall terminates once we are done"
        (reset! live? false)
        (is (= [[1 1]
                [2 2]] (doall log)))))))
