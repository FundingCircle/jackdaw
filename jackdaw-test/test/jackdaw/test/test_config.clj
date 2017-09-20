(ns jackdaw.test.test-config
  (:require
   [clojure.test :refer :all]
   [jackdaw.test.config :as config]
   [jackdaw.test.fs :as fs]
   [jackdaw.test.fixtures :as fix]))

(def env {:zookeeper-connect "localhost:2181"
          :bootstrap-servers "localhost:9092"
          :schema-registry-url "http://127.0.0.1:8081"
          :kafka-connect-host "localhost"
          :kafka-connect-port "8083"})

(def zk-connect (env :zookeeper-connect))

(def zookeeper
  {"zookeeper.connect"            zk-connect})

(def broker
  {"zookeeper.connect"            zk-connect
   "broker.id"                    "0"
   "advertised.host.name"         (-> (env :bootstrap-servers)
                                      (config/host-port)
                                      :host)
   "auto.create.topics.enable"    "true"
   "offsets.topic.num.partitions" "1"
   "log.dirs"                     (fs/tmp-dir "kafka-log")
   "controlled.shutdown.enable" "false"})

(def consumer
  {"bootstrap.servers"     (env :bootstrap-servers)
   "group.id"              "test"
   "key.deserializer"      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"    "org.apache.kafka.common.serialization.StringDeserializer"
   "metadata.max.age.ms"   "1000" ;; usually this is 5 minutes
   "auto.offset.reset"     "earliest"
   "enable.auto.commit"    "true"})

(def producer
  {"bootstrap.servers" (env :bootstrap-servers)
   "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"})

(def schema-registry
  {"listeners"                 (env :schema-registry-url)
   "kafkastore.connection.url" (env :zookeeper-connect)
   "kafkastore.topic"          "_schemas"})

(defn parse-int [x]
  (Integer/parseInt x 10))

(def kafka-connect-port (or (some-> (env :kafka-connect-port) parse-int) (int 48484)))
(def kafka-connect-host (or (env :kafka-connect-host) "localhost"))

(def kafka-connect-worker-config
  {"bootstrap.servers" (env :bootstrap-servers)
   "group.id" "kafka.test"
   "key.converter" "org.apache.kafka.connect.json.JsonConverter"
   "key.converter.schemas.enable" false
   "value.converter" "org.apache.kafka.connect.json.JsonConverter"
   "value.converter.schemas.enable" false
   "rest.host.name" "0.0.0.0"
   "rest.port" kafka-connect-port
   "rest.advertised.port" kafka-connect-port
   "rest.advertised.host.name" kafka-connect-host
   "internal.key.converter" "org.apache.kafka.connect.json.JsonConverter"
   "internal.value.converter" "org.apache.kafka.connect.json.JsonConverter"
   "internal.key.converter.schemas.enable" false
   "internal.value.converter.schemas.enable" false
   "config.storage.topic" "kafka-connect-config-kafkatest"
   "offset.storage.topic" "kafka-connect-offsets-kafkatest"
   "status.storage.topic" "kafka-connect-status-kafkatest"
   "offset.flush.interval.ms" 2000})
