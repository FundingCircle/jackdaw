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
  (testing "boolean"
    (let [avro-schema (parse-schema {:type "boolean"})
          schema-type (avro2/schema-type avro-schema)
          clj-data true
          avro-data true]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "double"
    (let [avro-schema (parse-schema {:type "double"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2.0
          avro-data 2.0]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "float"
    (let [avro-schema (parse-schema {:type "float"})
          schema-type (avro2/schema-type avro-schema)
          clj-data (float 2)
          avro-data (float 2)]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "int"
    (let [avro-schema (parse-schema {:type "int"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2
          avro-data 2]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "long"
    (let [avro-schema (parse-schema {:type "long"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2
          avro-data 2]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "string"
    (let [avro-schema (parse-schema {:type "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data "hello"
          avro-data "hello"]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "null"
    (let [avro-schema (parse-schema {:type "null"})
          schema-type (avro2/schema-type avro-schema)
          clj-data nil
          avro-data nil]
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "array"
    (let [avro-schema (parse-schema {:type "array", :items "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data ["hello"]
          avro-data (GenericData$Array. ^Schema avro-schema
                                        ^Collection clj-data)]
      (is (= avro-data (avro2/clj->avro schema-type clj-data))))))

(deftest avro->clj
  (testing "boolean"
    (let [avro-schema (parse-schema {:type "boolean"})
          schema-type (avro2/schema-type avro-schema)
          clj-data true
          avro-data true]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "double"
    (let [avro-schema (parse-schema {:type "double"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2.0
          avro-data 2.0]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "float"
    (let [avro-schema (parse-schema {:type "float"})
          schema-type (avro2/schema-type avro-schema)
          clj-data (float 2)
          avro-data (float 2)]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "int"
    (let [avro-schema (parse-schema {:type "int"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2
          avro-data 2]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "long"
    (let [avro-schema (parse-schema {:type "long"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2
          avro-data 2]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "string"
    (let [avro-schema (parse-schema {:type "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data "hello"
          avro-data "hello"]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "null"
    (let [avro-schema (parse-schema {:type "null"})
          schema-type (avro2/schema-type avro-schema)
          clj-data nil
          avro-data nil]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))))
  (testing "array"
    (let [avro-schema (parse-schema {:type "array", :items "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data ["a" "b" "c"]
          avro-data (GenericData$Array. ^Schema avro-schema
                                        ^Collection clj-data)]
      (is (= clj-data (avro2/avro->clj schema-type avro-data))))))
