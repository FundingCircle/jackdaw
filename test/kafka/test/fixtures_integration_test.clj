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
  (let [result (fix/publish! :foo {:topic "foo"
                                   :key "1"
                                   :value "bar"})
        result-log (->> (fix/log-seq :foo)
                        (map client/record))]

    (let [result (client/send! producer "1" "bar")]

    (testing "consume!"
      (is (= {:topic "foo"
              :key "1"
              :value "bar"}
             (-> (first result-log)
                 (select-keys [:topic :key :value])))))))
