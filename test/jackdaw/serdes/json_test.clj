(ns jackdaw.serdes.json-test
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :as ct :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [jackdaw.serdes.json :as jsj]))

(set! *warn-on-reflection* false)

(def string-bytes-roundtrip-property
  "A strings `s' should be the same after writing to a byte array and reading
  back as a string."
  (prop/for-all [s gen/string]
                (= s (jsj/from-bytes (jsj/to-bytes s)))))

(defspec string-roundtrip-test 100 string-bytes-roundtrip-property)

(deftest json-roundtrip-test
  (testing "JSON string is the same after serialization and deserialization."
    (let [s (slurp (io/resource "resources/pass1.json"))]
      (is (= (json/read-str s)
             (json/read-str (->> (.serialize (jsj/serializer) nil s)
                                 (.deserialize (jsj/deserializer) nil))))))))

(deftest reverse-json-roundtrip-test
  (testing "JSON bytes are the same after deserialization and serialization."
    (let [s (slurp (io/resource "resources/pass1.json"))
          b (.serialize (jsj/serializer) nil {:foo_bar "baz"})]
      (is (= (into [] b)
             (into [] (->> (.deserialize (jsj/deserializer) nil b)
                           (.serialize (jsj/serializer) nil))))))))
