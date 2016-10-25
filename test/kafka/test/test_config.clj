(ns kafka.test.test-config
  (:require
   [clojure.test :refer :all]
   [environ.core :refer [env]]
   [kafka.test.config :as config]
   [kafka.test.fs :as fs]
   [kafka.test.fixtures :as fix]))

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
   "log.dirs"                     (fs/tmp-dir "kafka-log")})

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
