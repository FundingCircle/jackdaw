(ns kafka-serdes.avro
  (:refer-clojure)
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
   (if (map? data)
     (avro/map->generic-record schema data)
     (throw (IllegalArgumentException. "Not a Clojure map")))))

(deftype CljKafkaAvroSerializer [^Serializer serializer ^String schema]
  Serializer
  (close [this]
    (.close serializer))
  (configure [this configs key?]
    (.configure serializer configs key?))
  (serialize [this topic data]
    (.serialize serializer topic (avro-record data schema))))

(defn avro-serializer
  "Create an Avro serializer"
  [client schema]
  (CljKafkaAvroSerializer. (KafkaAvroSerializer. client) schema))

(defn avro-deserializer
  "Create an Avro deserializer"
  [client]
  (KafkaAvroDeserializer. client))

(defn avro-serde
  "Create an Avro SerDe."
  [client schema]
  (Serdes/serdeFrom (avro-serializer client schema)
                    (avro-deserializer client)))
