(ns kafka.serdes.uuid-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :as ct :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [kafka.serdes.uuid :refer :all]))

(def uuid-bytes-roundtrip-property
  "A UUID should be the same after serialization and deserialization."
  (prop/for-all [u gen/uuid]
                (= u
                   (->> u
                        (.serialize (uuid-serializer) nil)
                        (.deserialize (uuid-deserializer) nil)))))

(def uuid-bytes-reverse-roundtrip-property
  "UUID bytes are the same after deserialization and serialization."
  (prop/for-all [u gen/uuid]
                (let [b (.serialize (uuid-serializer) nil u)]
                  (= (into [] b)
                     (into [] (->> b
                                   (.deserialize (uuid-deserializer) nil)
                                   (.serialize (uuid-serializer) nil)))))))

(defspec uuid-roundtrip-test 100 uuid-bytes-roundtrip-property)

(defspec uuid-reverse-roundtrip-test 100 uuid-bytes-reverse-roundtrip-property)
