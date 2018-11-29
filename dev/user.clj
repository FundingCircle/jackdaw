(ns user
  (:require [jackdaw.client :as jc]
            [jackdaw.serdes :as js]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.avro :as js.a]))

(def +local-schema-registry+
  {:avro.schema-registry/url "http://localhost:8081"})

(def +local-kafka+
  "A Kafka consumer or streams config."
  (let [id (str "dev-" (java.util.UUID/randomUUID))]
    {"replication.factor" "1", "group.id" id, "application.id" id,
     "bootstrap.servers"  "localhost:9092"
     "zookeeper.connect"  "localhost:2181"
     "request.timeout.ms" "1000"}))

(def +local-zookeeper+
  +local-kafka+)

(def +avro-type-registry+
  (merge js.a/+base-schema-type-registry+
         js.a/+UUID-type-registry+))

(def +admin-client+
  (ja/->AdminClient +local-kafka+))
