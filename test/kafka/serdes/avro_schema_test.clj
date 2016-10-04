(ns kafka.serdes.avro-schema-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [kafka.serdes.avro-schema :as avro-schema]))

(def schema (slurp "test/resources/example_schema.avsc"))

(def record (edn/read-string (slurp "test/resources/example_record.edn")))

(deftest map-roundtrip-test
  (testing "Map is the same after conversion to generic record and back"
    (is (= record
           (-> (avro-schema/map->generic-record schema record)
               (avro-schema/generic-record->map))))))
