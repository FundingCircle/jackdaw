(ns jackdaw.serdes.avro2-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.serdes.avro2 :as avro2]
            [clojure.data.json :as json])
  (:import (org.apache.avro Schema$Parser Schema)
           (org.apache.avro.generic GenericData$Array GenericData$Record)
           (java.util Collection HashMap)))

(defn parse-schema [clj-schema]
  (.parse (Schema$Parser.) ^String (json/write-str clj-schema)))

(deftest clj->avro
  (testing "string"
    (let [avro-schema (parse-schema {:type "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data "hello"
          avro-data "hello"]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "boolean"
    (let [avro-schema (parse-schema {:type "boolean"})
          schema-type (avro2/schema-type avro-schema)
          clj-data true
          avro-data true]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "array"
    (let [avro-schema (parse-schema {:type "array", :items "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data ["hello"]
          avro-data (GenericData$Array. ^Schema avro-schema
                                        ^Collection clj-data)]
      (is (= avro-data (avro2/clj->avro schema-type clj-data))))))

(deftest avro->clj
  (testing "string"
    (let [avro-schema (parse-schema {:type "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data "hello"
          avro-data "hello"]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "boolean"
    (let [avro-schema (parse-schema {:type "boolean"})
          schema-type (avro2/schema-type avro-schema)
          clj-data true
          avro-data true]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "array"
    (let [avro-schema (parse-schema {:type "array", :items "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data ["a" "b" "c"]
          avro-data (GenericData$Array. ^Schema avro-schema
                                        ^Collection clj-data)]
      (is (= clj-data (avro2/avro->clj schema-type avro-data))))))
