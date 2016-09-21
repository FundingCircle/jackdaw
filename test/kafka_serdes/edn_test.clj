(ns kafka-serdes.json-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.clojure-test :as ct :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [kafka-serdes.edn :refer :all]
   [clojure.edn :as edn]))

;; (def edn-bytes-roundtrip-property
;;   "An edn form should be the same after writing to a byte array and reading
;;   back as a string."
;;   (prop/for-all [edn gen/any-printable]
;;                 (= edn
;;                    (->> (.serialize edn-serializer nil (edn/read-string (str edn)))
;;                         (.deserialize edn-deserializer nil)))))

;; (defspec edn-roundtrip-test 100 edn-bytes-roundtrip-property)
