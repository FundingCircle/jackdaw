(ns jackdaw.streams.interop-test
  (:require
    [clojure.test :refer :all]
    [jackdaw.streams.interop :as interop])
  (:import
    (org.apache.kafka.common.serialization
      Serdes)))

(deftest topic->materialized-test
  (testing "materialized with serde only doesn't smoke"
    (is (interop/topic->materialized {:key-serde (Serdes/String)
                                      :value-serde (Serdes/String)})))
  (testing "materialized with topic name and serde doesn't smoke"
    (is (interop/topic->materialized {:key-serde (Serdes/String)
                                      :value-serde (Serdes/String)}))))

