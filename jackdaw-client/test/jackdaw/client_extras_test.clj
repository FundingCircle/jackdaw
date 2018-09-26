(ns jackdaw.client-extras-test
 (:require
   [accounting-common.fixture :as ac-fix]
   [clojure.test :refer :all]
   [jackdaw.client :as jc]
   [jackdaw.client.extras :as jce]
   [ledger.config :as config]
   [ledger.fixture :as fix]
   [accounting-common.gen-value :as gen-value]
   [ledger.test-config :as test-config]
   [ledger.topics :as topics]))

(when (ac-fix/topology-fixtures?)
  (use-fixtures :each (join-fixtures
                       [(ac-fix/topics-fixture (config/topics) (test-config/test-producer))])))

(deftest ^:integration record-map-test
  (testing "record-map predicts the correct partition"
    (let [messages (repeatedly 10 gen-value/ledger-entry-request)]
      (with-open [p (jc/producer (test-config/test-producer) (topics/ledger-entry-request))]
        (doseq [m messages
                :let [metadata (->> (jc/producer-record (topics/ledger-entry-request) (:customer-account-id m) m)
                                    (jc/send! p)
                                    deref
                                    jc/record-metadata)
                      expected-envelope (jc/producer-record
                                          (topics/ledger-entry-request)
                                          (:partition metadata)
                                          (:customer-account-id m)
                                          m)]]
          (is (= expected-envelope (jce/record-map (topics/ledger-entry-request) m))))))))

