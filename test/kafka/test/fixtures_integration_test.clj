(ns kafka.test.fixtures-integration-test
  (:require
   [clojure.test :refer :all]
   [kafka.client :as client]
   [kafka.zk :as zk]
   [kafka.test.config :as config]
   [kafka.test.fs :as fs]
   [kafka.test.fixtures :as fix]
   [kafka.test.test-config :as test-config]))

(use-fixtures :once
  (join-fixtures [(fix/kafka-platform test-config/zookeeper
                                      test-config/broker
                                      true)]))

(def poll-timeout-ms 1000)
(def consumer-timeout-ms 5000)

(deftest ^:integration integration-test
  (with-open [producer (client/producer test-config/producer "foo")
              consumer (client/consumer test-config/consumer "foo")]

    (let [result (client/send! producer "1" "bar")]

      (testing "publish!"
        (are [key] (get (client/metadata @result) key)
          :offset
          :topic
          :toString
          :partition
          :checksum
          :serializedKeySize
          :serializedValueSize
          :timestamp))

      (testing "consume!"
        (is (= ["1" "bar"]
               (first (client/log-messages consumer 1000 5000))))))))
