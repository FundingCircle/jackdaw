(ns kafka.log-messages-test
  (:require
   [clojure.test :refer :all]
   [kafka.client :as client])
  (:import
   (java.util.concurrent LinkedBlockingQueue TimeUnit)
   (org.apache.kafka.common TopicPartition)
   (org.apache.kafka.clients.consumer Consumer
                                      ConsumerRecord
                                      ConsumerRecords)))

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

(deftest test-log-messages
  (let [q (LinkedBlockingQueue.)
        consumer (mock-consumer "foo" q)
        live? (atom true)
        done? (fn []
                @live?)
        log (client/log-messages consumer 1000 done?)]
    (try
      (let [result (poll-result "foo" [[1 1]
                                       [2 2]])]

        (testing "can fetch items delivered to a topic"
          (.put q result)
          (let [[a b] (take 2 log)]
            (is (= [1 1] a))
            (is (= [2 2] b)))))
        
      (finally
        (reset! live? false)
        (testing "doall terminates once we are done"
          (is (= [[1 1]
                  [2 2]] (doall log))))))))



