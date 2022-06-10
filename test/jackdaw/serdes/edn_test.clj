(ns jackdaw.serdes.edn-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [jackdaw.serdes.edn :as jse]))

(set! *warn-on-reflection* false)

(defspec edn-roundtrip-test 20
  (testing "EDN data is the same after serialization and deserialization."
    (prop/for-all [x (gen/fmap #(apply str %) (gen/vector gen/char-alpha 100))]
      (is (= x (->> (.serialize (jse/serializer) nil x)
                    (.deserialize (jse/deserializer) nil)))))))

(defspec edn-reverse-roundtrip-test 20
  (testing "EDN data is the same after deserialization and serialization."
    (prop/for-all [x (gen/fmap #(apply str %) (gen/vector gen/char-alpha 100))]
      (let [bytes (.serialize (jse/serializer) nil x)]
        (is (= (seq bytes)
               (seq (->> (.deserialize (jse/deserializer) nil bytes)
                         (.serialize (jse/serializer) nil)))))))))

(defspec edn-print-length-test 20
  (testing "EDN data is the same after serialization and deserialization with *print-length*."
    (binding [*print-length* 100]
      (prop/for-all [x (gen/vector gen/int (inc *print-length*))]
        (is (= x (->> (.serialize (jse/serializer) nil x)
                      (.deserialize (jse/deserializer) nil))))))))

(defmethod print-method java.net.URI
  [obj writer]
  (.write writer "#jackdaw/uri ")
  (.write writer (pr-str (str obj))))

(defspec edn-roundtrip-custom-reader 20
  (testing "custom EDN reader"
    (let [opts {:readers {'jackdaw/uri #(java.net.URI. %)}}]
      (prop/for-all [x (s/gen uri?)]
        (is (= x (->> (.serialize (jse/serializer) nil x)
                      (.deserialize (jse/deserializer opts) nil))))))))
