(ns kafka.serdes.avro-schema-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [kafka.serdes.avro-schema :refer :all]))

(def schema (slurp (io/resource "resources/example_schema.avsc")))

(def record (edn/read-string (slurp (io/resource "resources/example_record.edn"))))

(deftest map-roundtrip-test
  (testing "Map is the same after conversion to generic record and back"
    (is (= record
           (-> (map->generic-record schema record)
               (generic-record->map))))))
