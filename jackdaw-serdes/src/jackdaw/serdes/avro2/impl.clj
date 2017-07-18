(ns jackdaw.serdes.avro2.impl
  (:require [clojure.data.json :as json]
            [clojure.future :refer [double? boolean? bytes?]])
  (:import (org.apache.avro Schema Schema$Type Schema$Parser)
           (java.util UUID Map)
           (org.apache.avro.generic GenericRecord GenericData$Record)
           (org.apache.kafka.common.serialization Serdes Serializer Deserializer)
           (io.confluent.kafka.serializers KafkaAvroSerializer KafkaAvroDeserializer)))

(defn logical-type-name [schema]
  (-> schema (.getProps) (.get "logicalType")))

(defn base-type-name [schema]
  (-> schema (.getType) (.getName)))

(def parse-schema-str
  (memoize (fn [schema-str]
             (.parse (Schema$Parser.) ^String schema-str))))

(defn new-serializer [{:keys [key? convert-fn registry-client base-config]
                       :as opts}]
  (let [base-serializer (KafkaAvroSerializer. registry-client)]
    (reify Serializer
      (close [_]
        (.close base-serializer))
      (configure [_ _ _]
        (.configure base-serializer base-config key?))
      (serialize [_ topic clj-data]
        (.serialize base-serializer topic (convert-fn clj-data))))))

(defn new-deserializer [{:keys [key? convert-fn registry-client base-config]
                         :as opts}]
  (let [base-deserializer (KafkaAvroDeserializer. registry-client)]
    (reify Deserializer
      (close [_]
        (.close base-deserializer))
      (configure [_ _ _]
        (.configure base-deserializer base-config key?))
      (deserialize [_ topic raw-data]
        (convert-fn (.deserialize base-deserializer topic raw-data))))))
