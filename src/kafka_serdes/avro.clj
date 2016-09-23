(ns kafka-serdes.avro
  (:require
   [kafka-serdes.avro-schema :as avro])
  (:import
   (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient
                                             MockSchemaRegistryClient)
   (io.confluent.kafka.serializers KafkaAvroDeserializer
                                   KafkaAvroSerializer)
   (org.apache.avro.generic GenericContainer)
   (org.apache.kafka.common.serialization Deserializer
                                          Serializer
                                          Serdes)))

(set! *warn-on-reflection* true)

(defn avro-record
  "Create an Avro-serializable object."
  ([data]
   (if (some #{(type data)}
             [nil Boolean Integer Long Float Double CharSequence (type (byte-array 0)) GenericContainer])
     data
     (throw (IllegalArgumentException. "Not an primitive Avro type"))))
  ([data schema]
   (if schema
     (if (map? data)
       (avro/map->generic-record schema data)
       (throw (IllegalArgumentException. "Not a Clojure map")))
     (avro-record data))))

(deftype CljAvroSerializer [^Serializer serializer schema]
  Serializer
  (close [this]
    (.close serializer))
  (configure [this configs key?]
    (.configure serializer configs key?))
  (serialize [this topic msg]
    (.serialize serializer topic (avro-record msg schema))))

(defn avro-serializer
  "Makes an avro serializer."
  ([schema config key?]
   (avro-serializer nil schema config key?))
  ([registry-client schema {:keys [schema-registry-url]} key?]
   (let [serializer (KafkaAvroSerializer. registry-client)]
     (when schema-registry-url
       (let [config (java.util.HashMap.)]
         (.put config "schema.registry.url" schema-registry-url)
         (.configure serializer config key?)))
     (CljAvroSerializer. serializer schema))))

(defn avro-deserializer
  "Makes an avro deserializer"
  ([config key?]
   (avro-deserializer nil config key?))
  ([registry-client {:keys [schema-registry-url]} key?]
   (let [deserializer (KafkaAvroDeserializer. registry-client)]
     (when schema-registry-url
       (let [config (java.util.HashMap.)]
         (.put config "schema.registry.url" schema-registry-url)
         (.configure deserializer config key?)))
     deserializer)))

(defn avro-serde
  "Creates an avro serde."
  [config json-schema key?]
  (Serdes/serdeFrom (avro-serializer json-schema config key?)
                    (avro-deserializer config key?)))
