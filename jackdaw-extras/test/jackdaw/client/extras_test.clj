(ns jackdaw.client.extras-test
 (:require [clojure.data.json :as json]
           [clojure.test :refer :all]
           [jackdaw.admin.topic :as topic]
           [jackdaw.admin.zk :as zk]
           [jackdaw.client :as jc]
           [jackdaw.client.extras :as jce]
           [jackdaw.serdes :as serdes]
           [jackdaw.serdes.avro :as avro])
 (:import java.util.UUID
          org.apache.avro.Schema$Parser
          io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient))

(def zk-connect "localhost:2181")

(def producer-config {"bootstrap.servers" "localhost:9092"})

(defn arbitrary-string
  []
  (str (UUID/randomUUID)))

(defn test-topic
  [test-key topic-name]
  (let [test-schema (json/write-str
                     {:name "client_extras_test_schema"
                      :type :record
                      :fields [{:name "testkey"
                                :type :string}
                               {:name "testother"
                                :type :string}]})
        topic-config {:avro/schema test-schema
                      :key? false}
        schema-registry-config {:avro.schema-registry/url "http://localhost:8081"
                                :avro.schema-registry/client (MockSchemaRegistryClient.)}]
    {:jackdaw.topic/topic-name topic-name
     :jackdaw.topic/record-key :partition-key
     :jackdaw.serdes/key-serde (serdes/serde ::serdes/string)
     :jackdaw.serdes/value-serde (avro/avro-serde schema-registry-config
                                                  topic-config)
     :jackdaw.topic/partition-key test-key
     :jackdaw.topic/partitions 15}))

(defn random-message
  [topic-key]
  {topic-key (arbitrary-string)
   :testother (arbitrary-string)})

(def ^:dynamic *topic-info*
  {})

(defn generated-topic-and-schema-fixture
  "used to isolate topic, schema, serdes etc. scaffolding from the thing being  tested"
  [tests-fn]
  (let
    [topic-key :testkey
     topic-name  (str "jackdaw-client-extras-test-" (arbitrary-string))
     topic (test-topic topic-key topic-name)
     zk (zk/zk-utils zk-connect)]
    (binding [*topic-info*
              {:topic-key topic-key
               :topic-name topic-name
               :topic topic}]
      (topic/create! zk topic-name (:jackdaw.topic/partitions topic) 1 {})
      (try
       (tests-fn)
       (finally (topic/delete! zk topic-name))))))

(use-fixtures
 :once
 generated-topic-and-schema-fixture)

(deftest ^:integration record-map-test
  (testing "record-map predicts the correct partition"
    (let [{:keys [topic-key topic-name topic]} *topic-info*
          messages (repeatedly 10 #(random-message topic-key))]
       (with-open [p (jc/producer producer-config topic)]
         (doseq [m messages
                 :let [metadata (->> (jc/producer-record topic (topic-key m) m)
                                     (jc/send! p)
                                     deref
                                     jc/record-metadata)
                       expected-envelope (jc/producer-record
                                          topic
                                          (:partition metadata)
                                          (topic-key m)
                                          m)]]
           (is (= expected-envelope (jce/record-map topic m))))))))

