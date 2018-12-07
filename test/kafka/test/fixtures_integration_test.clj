(ns kafka.test.fixtures-integration-test
  (:require
   [kafka.admin :as admin]
   [kafka.core :as kafka]
   [kafka.zk :as zk]
   [kafka.test.config :as config]
   [kafka.test.fs :as fs]
   [kafka.test.fixtures :as fix]
   [clojure.test :refer :all]))

(use-fixtures :once (join-fixtures [(fix/kafka-platform config/broker)
                                    (fix/producer-registry {:foo config/producer})
                                    (fix/log-seqs {:foo config/consumer})]))

(deftest ^:integration integration-test
  (let [result (fix/publish! :foo {:topic "foo"
                                   :key "1"
                                   :value "bar"})
        result-log (->> (fix/log-seq :foo)
                        (map kafka/record))]

    (testing "publish!"
      (are [key] (get (kafka/metadata @result) key)
        :offset
        :topic
        :toString
        :partition
        :checksum
        :serializedKeySize
        :serializedValueSize
        :timestamp))

    (testing "consume!"
      (is (= {:topic "foo"
              :key "1"
              :value "bar"}
             (-> (first result-log)
                 (select-keys [:topic :key :value])))))))
