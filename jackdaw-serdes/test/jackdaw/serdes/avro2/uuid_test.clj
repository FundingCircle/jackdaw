(ns jackdaw.serdes.avro2.uuid-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.serdes.avro2 :as avro2]
            [jackdaw.serdes.avro2.uuid]
            [clojure.data.json :as json]
            [clj-uuid :as uuid]
            [clojure.java.io :as io])
  (:import (org.apache.avro Schema$Parser)
           (org.apache.avro.generic GenericData$Record)))

(defn parse-schema [clj-schema]
  (.parse (Schema$Parser.) ^String (json/write-str clj-schema)))

(deftest schema-type
  (testing "string base type"
    (let [avro-schema (parse-schema {:type "string"
                                     :name "id"
                                     :namespace "com.fundingcircle"
                                     :logicalType "jackdaw.serdes.avro.UUID"})
          schema-type (avro2/schema-type avro-schema)
          clj-data #uuid "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"
          avro-data "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "a record containing a string with UUID logicalType"
    (let [uuid-schema {:type "string"
                       :logicalType "jackdaw.serdes.avro.UUID"}
          record-schema (parse-schema {:type "record"
                                       :name "recordStringLogicalTypeTest"
                                       :namespace "com.fundingcircle"
                                       :fields [{:name "id"
                                                 :namespace "com.fundingcircle"
                                                 :type uuid-schema}]})
          schema-type (avro2/schema-type record-schema)
          id (uuid/v4)
          clj-data {:id id}
          avro-data (doto (GenericData$Record. record-schema)
                      (.put "id" (str id)))]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data))))))
