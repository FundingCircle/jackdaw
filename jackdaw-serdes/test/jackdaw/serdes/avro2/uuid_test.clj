(ns jackdaw.serdes.avro2.uuid-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.serdes.avro2 :as avro2]
            [jackdaw.serdes.avro2.uuid]
            [clojure.data.json :as json])
  (:import (org.apache.avro Schema$Parser)))

(defn parse-schema [clj-schema]
  (.parse (Schema$Parser.) ^String (json/write-str clj-schema)))

(deftest clj->avro
  (testing "string base type"
    (let [avro-schema (parse-schema {:type "string"
                                     :logicalType "jackdaw.serdes.avro.UUID"})
          schema-type (avro2/schema-type avro-schema)
          clj-data #uuid "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"
          avro-data "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"]
      (is (= avro-data (avro2/clj->avro schema-type clj-data))))))

(deftest avro->clj
  (testing "string base type"
    (let [avro-schema (parse-schema {:type "string"
                                     :logicalType "jackdaw.serdes.avro.UUID"})
          schema-type (avro2/schema-type avro-schema)
          clj-data #uuid "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"
          avro-data "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"]
      (is (= clj-data (avro2/avro->clj schema-type avro-data))))))
