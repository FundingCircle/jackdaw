(ns jackdaw.serdes.edn.nippy-test
  (:require [clojure.test :refer :all]
            [jackdaw.serdes.edn.nippy :as j.s.edn.nippy]
            [taoensso.nippy :as nippy]))

(deftest edn-nippy-roundtrip-test
  (testing "EDN data is the same after serialization and deserialization."
    (is (= nippy/stress-data-comparable
           (->> nippy/stress-data-comparable
                (.serialize (j.s.edn.nippy/serializer) nil)
                (.deserialize (j.s.edn.nippy/deserializer) nil))))))

(deftest edn-nippy-reverse-roundtrip-test
  (testing "EDN data is the same after deserialization and serialization."
    (let [bytes (.serialize (j.s.edn.nippy/serializer)
                            nil
                            nippy/stress-data-comparable)]
      (is (= (seq bytes)
             (seq (->> (.deserialize (j.s.edn.nippy/deserializer) nil bytes)
                       (.serialize (j.s.edn.nippy/serializer) nil))))))))
