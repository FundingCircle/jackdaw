(ns kafka.test.config
  (:require
   [clojure.string :as str]
   [kafka.test.fs :as fs]
   [environ.core :refer [env]])
  (:import
   (java.util Properties)))

(defn host-port [host-str]
  (try
   (let [[host port] (str/split host-str #"\:")]
      {:host host
       :port (Integer/parseInt port)})
    (catch Exception e
      (throw (ex-info "invalid host string: " {:host-str host-str} e)))))

(defn properties
  "Generate java.util.Properties for a clojure map
   If a `path` is supplied, generate properties only for the value
   obtained by invoking `(get-in m path)`."
  ([m]
   (properties m []))

  ([m path]
   (let [props (Properties. )]
     (doseq [[n v] (get-in m path)]
       (.setProperty props n v))
     props)))

(def broker
  {"zookeeper.connect"            (env :zookeeper-connect)
   "broker.id"                    "0"
   "advertised.host.name"         (-> (env :bootstrap-servers)
                                      (host-port)
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
