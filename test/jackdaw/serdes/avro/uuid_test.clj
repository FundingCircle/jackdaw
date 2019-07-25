(ns jackdaw.serdes.avro.uuid-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.serdes.avro :as avro]
            [clojure.data.json :as json]
            [clj-uuid :as uuid])
  (:import (org.apache.avro Schema$Parser)
           (org.apache.avro.generic GenericData$Record)
           (org.apache.avro.util Utf8)))

(def +registry+
  (merge avro/+base-schema-type-registry+
         avro/+UUID-type-registry+))

(def schema-type
  (avro/make-coercion-stack
   +registry+))

(defn parse-schema [clj-schema]
  (.parse (Schema$Parser.) ^String (json/write-str clj-schema)))

(deftest schema-type-jackdaw-uuid
  (testing "string base type"
    (let [avro-schema (parse-schema {:type "string"
                                     :name "id"
                                     :namespace "com.fundingcircle"
                                     :logicalType "jackdaw.serdes.avro.UUID"})
          schema-type (schema-type avro-schema)
          clj-data #uuid "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"
          avro-data (Utf8. "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb")]
      (is (avro/match-clj? schema-type clj-data))
      (is (avro/match-avro? schema-type avro-data))
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= (str avro-data) (avro/clj->avro schema-type clj-data [])))))
  (testing "a record containing a string with UUID logicalType"
    (let [uuid-schema {:type "string"
                       :logicalType "jackdaw.serdes.avro.UUID"}
          record-schema (parse-schema {:type "record"
                                       :name "recordStringLogicalTypeTest"
                                       :namespace "com.fundingcircle"
                                       :fields [{:name "id"
                                                 :namespace "com.fundingcircle"
                                                 :type uuid-schema}]})
          schema-type (schema-type record-schema)
          id (uuid/v4)
          clj-data {:id id}
          avro-data (doto (GenericData$Record. record-schema)
                      (.put "id" (str id)))]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"java.lang.String is not a valid type for uuid"
            (avro/clj->avro schema-type (update clj-data :id str) []))))))

(deftest schema-type-uuid
  (testing "string base type"
    (let [avro-schema (parse-schema {:type "string"
                                     :name "id"
                                     :namespace "com.fundingcircle"
                                     :logicalType "uuid"})
          schema-type (schema-type avro-schema)
          clj-data #uuid "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"
          avro-data (Utf8. "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb")]
      (is (avro/match-clj? schema-type clj-data))
      (is (avro/match-avro? schema-type avro-data))
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= (str avro-data) (avro/clj->avro schema-type clj-data [])))))
  (testing "a record containing a string with UUID logicalType"
    (let [uuid-schema {:type "string"
                       :logicalType "uuid"}
          record-schema (parse-schema {:type "record"
                                       :name "recordStringLogicalTypeTest"
                                       :namespace "com.fundingcircle"
                                       :fields [{:name "id"
                                                 :namespace "com.fundingcircle"
                                                 :type uuid-schema}]})
          schema-type (schema-type record-schema)
          id (uuid/v4)
          clj-data {:id id}
          avro-data (doto (GenericData$Record. record-schema)
                      (.put "id" (str id)))]
      (is (= clj-data (avro/avro->clj schema-type avro-data)))
      (is (= avro-data (avro/clj->avro schema-type clj-data [])))
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"java.lang.String is not a valid type for uuid"
            (avro/clj->avro schema-type (update clj-data :id str) []))))))
