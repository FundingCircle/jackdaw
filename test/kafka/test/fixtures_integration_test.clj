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
  (join-fixtures [(fix/zookeeper test-config/zookeeper)
                  (fix/broker test-config/broker)
                  (fix/schema-registry test-config/schema-registry)]))

(def poll-timeout-ms 1000)
(def consumer-timeout-ms 5000)

(defn fuse
  "Returns a function that throws an exception when called after some time has passed."
  [millis]
  (let [end (+ millis (System/currentTimeMillis))]
    (fn []
      (if (< end (System/currentTimeMillis))
        (throw (ex-info "Timer expired" {:millis millis}))
        true))))

(deftest ^:integration integration-test
  (let [topic {:topic.metadata/name "foo"}]
    (with-open [producer (client/producer test-config/producer)
                consumer (-> (client/consumer test-config/consumer)
                             (client/subscribe topic))]

    (testing "publish!"
      (let [result (client/send! producer (client/producer-record "foo" "1" "bar"))]
        (are [key] (get (client/metadata @result) key)
          :offset
          :topic
          :toString
          :partition
          :checksum
          :serializedKeySize
          :serializedValueSize
          :timestamp)))

    (testing "consume!"
      (let [[key val] (-> (client/log-messages consumer
                                               poll-timeout-ms
                                               (fuse consumer-timeout-ms))
                          first)]
        (is (= ["1" "bar"] [key val]))))))
