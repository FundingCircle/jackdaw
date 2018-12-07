(ns kafka.test.fixtures-integration-test
  (:require
   [clojure.test :refer :all]
   [kafka.admin :as admin]
   [kafka.client :as client]
   [kafka.zk :as zk]
   [kafka.test.config :as config]
   [kafka.test.fs :as fs]
   [kafka.test.fixtures :as fix]
   [kafka.test.test-config :as test-config]))

(use-fixtures :once (join-fixtures [(fix/kafka-platform test-config/broker true)
                                    (fix/producer-registry {:foo test-config/producer})
                                    (fix/consumer-registry {:foo test-config/consumer})]))

(deftest ^:integration integration-test
  (let [producer (fix/find-producer :foo)
        consumer (fix/find-consumer :foo)]

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
