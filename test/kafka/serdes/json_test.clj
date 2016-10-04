(ns kafka.serdes.json-test
  (:require
   [clojure.data.json :as json]
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
    (let [s (slurp "test/resources/pass1.json")]
      (is (= (json/read-str s)
             (json/read-str (->> (.serialize json-serializer nil s)
                                 (.deserialize json-deserializer nil))))))))
