(ns jackdaw.serdes.avro-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.serdes.avro :as avro]
            [clojure.data.json :as json]
            [clj-uuid :as uuid]
            [clojure.java.io :as io]
            [environ.core :as env]
            [jackdaw.serdes.avro :as avro])
  (:import (org.apache.avro Schema$Parser Schema)
           (org.apache.avro.generic GenericData$EnumSymbol GenericData$Record GenericData$Array)
           (org.apache.avro.util Utf8)
           (java.util Collection)))

(defn parse-schema [clj-schema]
  (.parse (Schema$Parser.) ^String (json/write-str clj-schema)))

(deftest schema-type
  (testing "boolean"
    (let [avro-schema (parse-schema {:type "boolean"})
          schema-type (avro/schema-type avro-schema)
          clj-data true
          avro-data true]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "double"
    (let [avro-schema (parse-schema {:type "double"})
          schema-type (avro/schema-type avro-schema)
          clj-data 2.0
          avro-data 2.0]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "float"
    (let [avro-schema (parse-schema {:type "float"})
          schema-type (avro/schema-type avro-schema)
          clj-data (float 2)
          avro-data (float 2)]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "int"
    (let [avro-schema (parse-schema {:type "int"})
          schema-type (avro/schema-type avro-schema)
          clj-data 2
          avro-data 2]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "long"
    (let [avro-schema (parse-schema {:type "long"
                                     :name "amount_cents"
                                     :namespace "com.fundingcircle"})
          schema-type (avro/schema-type avro-schema)
          clj-data 4
          avro-data (Integer. 4)]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "string"
    (let [avro-schema (parse-schema {:type "string"
                                     :name "postcode"
                                     :namespace "com.fundingcircle"})
          schema-type (avro/schema-type avro-schema)
          clj-data "test-string"
          avro-data "test-string"]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "unmarshalling a utf8 character set"
    (let [avro-schema (parse-schema {:namespace "com.fundingcircle"
                                     :name "euro"
                                     :type "string"})
          schema-type (avro/schema-type avro-schema)
          b (byte-array [0xE2 0x82 0xAC])
          utf8 (Utf8. b)]
      (is (= (String. b) (avro/avro->clj schema-type utf8)))))
  (testing "null"
    (let [avro-schema (parse-schema {:type "null"})
          schema-type (avro/schema-type avro-schema)
          clj-data nil
          avro-data nil]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "array"
    (let [avro-schema (parse-schema {:namespace "com.fundingcircle"
                                     :name "credit_score_guarantors"
                                     :type "array"
                                     :items "string"})
          schema-type (avro/schema-type avro-schema)
          clj-data ["0.4" "56.7"]
          avro-data (GenericData$Array. ^Schema avro-schema
                                        ^Collection clj-data)]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "map"
    (let [avro-schema (parse-schema {:type "map", :values "long"})
          schema-type (avro/schema-type avro-schema)
          clj-data {:foo 1 :bar 2}
          avro-data {"foo" 1 "bar" 2}]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "union"
    (let [avro-schema (parse-schema ["long" "string"])
          schema-type (avro/schema-type avro-schema)
          clj-data-long 123
          avro-data-long 123
          clj-data-string "hello"
          avro-data-string "hello"]
      (is (= clj-data-long (avro/avro->clj schema-type avro-data-long)))
      (is (= avro-data-long (avro/clj->avro schema-type clj-data-long)))
      (is (= clj-data-string (avro/avro->clj schema-type avro-data-string)))
      (is (= avro-data-string (avro/clj->avro schema-type clj-data-string)))))
  (testing "marshalling unrecognized union type throws exception"
    (let [avro-schema (parse-schema ["null" "long"])
          schema-type (avro/schema-type avro-schema)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"No matching union schema"
                            (avro/clj->avro schema-type "foo")))))
  (testing "enum"
    (let [enum-schema {:type "enum"
                       :name "industry_code_version"
                       :symbols ["SIC_2003"]}
          avro-schema (parse-schema {:type "record"
                                     :name "enumtest"
                                     :namespace "com.fundingcircle"
                                     :fields [{:name "industry_code_version"
                                               :type enum-schema}]})
          schema-type (avro/schema-type avro-schema)
          clj-data {:industry-code-version :SIC-2003}
          avro-enum (GenericData$EnumSymbol. avro-schema "SIC_2003")
          avro-data (doto (GenericData$Record. avro-schema)
                      (.put "industry_code_version" avro-enum))]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
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
          schema-type (avro/schema-type avro-schema)
          clj-data {:stringField "foo"
                    :longField 123
                    :recordField {:a 1}}
          avro-data (doto (GenericData$Record. avro-schema)
                      (.put "stringField" "foo")
                      (.put "longField" 123)
                      (.put "recordField"
                            (doto (GenericData$Record. nested-schema-parsed)
                              (.put "a" 1))))]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data)))))
  (testing "marshalling record with unknown field triggers error"
    (let [avro-schema (parse-schema {:type "record"
                                     :name "Foo"
                                     :fields [{:name "bar" :type "string"}]})
          schema-type (avro/schema-type avro-schema)]
      (is (thrown-with-msg? AssertionError
                            #"Field garbage not known in Foo"
                            (avro/clj->avro schema-type {:garbage "yolo"}))))))
