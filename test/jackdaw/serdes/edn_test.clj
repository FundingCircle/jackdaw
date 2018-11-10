(ns jackdaw.serdes.edn-test
  (:require [clojure.test :refer :all]
            [clojure.test.check
             [clojure-test :as ct :refer [defspec]]
             [generators :as gen]
             [properties :as prop]]
            [jackdaw.serdes.edn :refer :all]
            [taoensso.nippy :as nippy]))

(deftest edn-roundtrip-test
  (testing "EDN data is the same after serialization and deserialization."
    (is (= nippy/stress-data-comparable
           (->> nippy/stress-data-comparable
                (.serialize (edn-serializer) nil)
                (.deserialize (edn-deserializer) nil))))))

(deftest edn-reverse-roundtrip-test
  (testing "EDN data is the same after deserialization and serialization."
    (let [bytes (.serialize (edn-serializer) nil nippy/stress-data-comparable)]
      (is (= (seq bytes)
             (seq (->> (.deserialize (edn-deserializer) nil bytes)
                       (.serialize (edn-serializer) nil))))))))
