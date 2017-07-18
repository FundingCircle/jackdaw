(ns jackdaw.serdes.avro2-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.serdes.avro2 :as avro2]
            [clojure.data.json :as json])
  (:import (org.apache.avro Schema$Parser Schema)
           (org.apache.avro.generic GenericData$Array GenericData$Record)
           (java.util Collection HashMap)))

(defn parse-schema [clj-schema]
  (.parse (Schema$Parser.) ^String (json/write-str clj-schema)))

(deftest schema-type
  (testing "boolean"
    (let [avro-schema (parse-schema {:type "boolean"})
          schema-type (avro2/schema-type avro-schema)
          clj-data true
          avro-data true]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "double"
    (let [avro-schema (parse-schema {:type "double"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2.0
          avro-data 2.0]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "float"
    (let [avro-schema (parse-schema {:type "float"})
          schema-type (avro2/schema-type avro-schema)
          clj-data (float 2)
          avro-data (float 2)]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "int"
    (let [avro-schema (parse-schema {:type "int"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2
          avro-data 2]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "long"
    (let [avro-schema (parse-schema {:type "long"})
          schema-type (avro2/schema-type avro-schema)
          clj-data 2
          avro-data 2]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "string"
    (let [avro-schema (parse-schema {:type "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data "hello"
          avro-data "hello"]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "null"
    (let [avro-schema (parse-schema {:type "null"})
          schema-type (avro2/schema-type avro-schema)
          clj-data nil
          avro-data nil]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "array"
    (let [avro-schema (parse-schema {:type "array", :items "string"})
          schema-type (avro2/schema-type avro-schema)
          clj-data ["a" "b" "c"]
          avro-data (GenericData$Array. ^Schema avro-schema
                                        ^Collection clj-data)]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "map"
    (let [avro-schema (parse-schema {:type "map", :values "long"})
          schema-type (avro2/schema-type avro-schema)
          clj-data {:a 1 :b 2}
          avro-data (doto (HashMap.)
                      (.put "a" 1)
                      (.put "b" 2))]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "record"
    (let [nested-schema-json {:name "nestedRecord"
                              :type "record"
                              :fields [{:name "a"
                                        :type "long"}]}
          nested-schema-parsed (parse-schema nested-schema-json)
          avro-schema (parse-schema {:name "testRecord"
                                     :type "record"
                                     :fields [{:name "stringField"
                                               :type "string"}
                                              {:name "longField"
                                               :type "long"}
                                              {:name "recordField"
                                               :type nested-schema-json}]})
          schema-type (avro2/schema-type avro-schema)
          clj-data {:stringField "foo"
                    :longField 123
                    :recordField {:a 1}}
          avro-data (doto (GenericData$Record. avro-schema)
                      (.put "stringField" "foo")
                      (.put "longField" 123)
                      (.put "recordField"
                            (doto (GenericData$Record. nested-schema-parsed)
                              (.put "a" 1))))]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data))))))
