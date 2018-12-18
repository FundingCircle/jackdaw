(ns jackdaw.serdes.edn-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [jackdaw.serdes.edn :as jse]))

(defspec edn-roundtrip-test 20
  (testing "EDN data is the same after serialization and deserialization."
    (prop/for-all [x gen/any-printable]
      (is (= x (->> (.serialize (jse/serializer) nil x)
                    (.deserialize (jse/deserializer) nil)))))))

(defspec edn-reverse-roundtrip-test 20
  (testing "EDN data is the same after deserialization and serialization."
    (prop/for-all [x gen/any-printable]
      (let [bytes (.serialize (jse/serializer) nil x)]
        (is (= (seq bytes)
               (seq (->> (.deserialize (jse/deserializer) nil bytes)
                         (.serialize (jse/serializer) nil)))))))))
