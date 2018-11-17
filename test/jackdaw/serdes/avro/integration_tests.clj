(ns jackdaw.serdes.avro.integration-tests
  (:require [clj-uuid :as uuid]
            [clojure.core.cache :as cache]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [jackdaw.client :as jc]
            [jackdaw.client.extras :as jce]
            [jackdaw.serdes :as js]
            [jackdaw.serdes.avro :as avro]
            [jackdaw.serdes.avro.schema-registry :as reg])
  (:import [org.apache.avro Schema$Parser]
           [org.apache.avro.generic GenericData$Record]
           [org.apache.kafka.common.serialization Serde]))

(def +type-registry+
  (merge avro/+base-schema-type-registry+
         avro/+UUID-type-registry+))

(def +topic-config+
  {:key?        false
   :avro/schema (slurp (io/resource "resources/example_schema.avsc"))})

(def +real-schema-registry+
  (let [url "http://localhost:8081"]
    {:avro.schema-registry/url    url
     :avro.schema-registry/client (reg/client url 16)}))

(def +mock-schema-registry+
  (merge +real-schema-registry+
         {:avro.schema-registry/client (reg/mock-client)}))

(deftest mock-schema-registry
  (testing "schema can be serialized by registry client"
    (let [serde ^Serde (avro/avro-serde +type-registry+ +mock-schema-registry+ +topic-config+)]
      (let [msg {:customer-id (uuid/v4)
                 :address     {:value    "foo"
                               :key-path "foo.bar.baz"}}]
        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))

(deftest ^:integration real-schema-registry
  (testing "schema registry set in config"
    (let [serde ^Serde (avro/avro-serde +type-registry+ +real-schema-registry+ +topic-config+)]
      (let [msg {:customer-id (uuid/v4)
                 :address     {:value    "foo"
                               :key-path "foo.bar.baz"}}]
        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))

;;;; Client integration tests against real Kafka through a real topic

(def +local-kafka+
  "A Kafka consumer or streams config."
  (let [id (str "dev-" (java.util.UUID/randomUUID))]
    {"replication.factor" "1", "group.id" id, "application.id" id,
     "bootstrap.servers"  "localhost:9092"
     "zookeeper.connect"  "localhost:2181"
     "request.timeout.ms" "1000"}))

;;;; Schemas

(def +test-schema-v1+ (slurp (io/resource "foo-1.avsc")))
(def +test-schema-v2+ (slurp (io/resource "foo-2.avsc")))
(def +test-schema-v3+ (slurp (io/resource "foo-3.avsc")))

;;;; "versioned" topic configs

(def serde*
  (partial avro/avro-serde +type-registry+ +real-schema-registry+))

(deftest ^:integration schema-evolution-test
  (testing "serialize then deserialize several serde versions"
    (let [v1-cache
          (atom (cache/lru-cache-factory {}))

          test-topic-v1
          {:jackdaw.topic/topic-name
           (str "test-topic-" (uuid/v4))

           :jackdaw.serdes/key-serde
           (js/serde ::js/string)

           :jackdaw.serdes/value-serde
           (serde*
            {:key?                false
             :avro/schema         +test-schema-v1+
             :avro/coercion-cache v1-cache})}

          test-topic-v2
          (merge test-topic-v1
                 {:jackdaw.serdes/value-serde
                  (serde* {:key? false, :avro/schema +test-schema-v2+})})

          test-topic-v3
          (merge test-topic-v1
                 {:jackdaw.serdes/value-serde
                  (serde* {:key? false, :avro/schema +test-schema-v3+})})

          topic+record
          [[test-topic-v1 {:a "foo"}]
           [test-topic-v2 {:a "foo", :b "bar"}]
           [test-topic-v3 {:a "foo", :b "bar", :c (uuid/v4)}]]]

      (doseq [[t r] topic+record]
        (with-open [p (jc/producer +local-kafka+ t)]
          @(jc/send! p (jc/producer-record t 0 (:a r) r))))

      (with-open [c (-> (jc/subscribed-consumer +local-kafka+ test-topic-v1)
                        (jc/seek-to-beginning-eager))]
        (doseq [[[_ r] {r' :value}]
                (map vector
                     topic+record
                     (doall (jce/log-seq-until-inactivity c 1000)))]
          (is (= r r') "Record didn't round trip!")))

      (is (= 3 (count (keys @v1-cache)))
          "Expect this cache will have all three schema versions now"))))
