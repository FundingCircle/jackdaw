(ns kafka.serdes.json-test
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test :as ct :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [kafka.serdes.json :refer :all]))

(def string-bytes-roundtrip-property
  "A strings `s' should be the same after writing to a byte array and reading
  back as a string."
  (prop/for-all [s gen/string]
                (= s
                   (-> s
                       string-to-bytes
                       bytes-to-string))))

(defspec string-roundtrip-test 100 string-bytes-roundtrip-property)

(deftest json-roundtrip-test
  (testing "JSON string is the same after serialization and deserialization."
    (let [s (slurp (io/resource "resources/pass1.json"))]
      (is (= (json/read-str s)
             (json/read-str (->> (.serialize (json-serializer) nil s)
                                 (.deserialize (json-deserializer) nil))))))))

(deftest reverse-json-roundtrip-test
  (testing "JSON bytes are the same after deserialization and serialization."
    (let [s (slurp (io/resource "resources/pass1.json"))
          b (.serialize (json-serializer) nil {:foo_bar "baz"})]
      (is (= (into [] b)
             (into [] (->> (.deserialize (json-deserializer) nil b)
                           (.serialize (json-serializer) nil))))))))
