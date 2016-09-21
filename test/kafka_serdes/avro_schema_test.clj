(ns kafka-serdes.avro-schema-test
  (:require
   [clojure.data.json :as json]
   [clojure.edn :as edn]
   [clojure.test :refer :all]
   [kafka-serdes.avro-schema :refer :all]))

(def schema (slurp "test/resources/example_schema.avsc"))

(def record (edn/read-string (slurp "test/resources/example_record.edn")))

(deftest map-roundtrip-test
  (testing "Map is the same after conversion to generic record and back"
    (is (= record
           (-> (map->generic-record schema record)
               (generic-record->map))))))
