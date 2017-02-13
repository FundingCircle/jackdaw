(ns kafka.serdes.avro
  (:require [clj-uuid :as uuid]
            [kafka.serdes.avro-schema :as avro-schema]
            [kafka.serdes.registry :as registry]
            [environ.core :as env])
  (:import [io.confluent.kafka.serializers KafkaAvroDeserializer KafkaAvroSerializer]
           [org.apache.kafka.common.serialization Serdes Serializer Deserializer]
           [io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient]))

(set! *warn-on-reflection* true)

(defn avro-record
  "Creates an avro record for serialization, from a clojure type.

  - Clojure maps are converted to a GenericDataRecord.
  - UUIDs are converted to strings if schema is a logical UUID.
  - Primitive types are passed through to the avro serializer."
  [msg schema]
  (cond
    (map? msg) (avro-schema/map->generic-record schema msg)
    (and (uuid/uuid? msg)
         (avro-schema/uuid-schema? (avro-schema/parse-schema schema))) (str msg)
    :else msg))

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
  ([registry-client schema base-config key?]
   (let [serializer (KafkaAvroSerializer. registry-client)]
     (let [config (java.util.HashMap.)]
       (.put config "schema.registry.url" (registry/url base-config))
       (.configure serializer config key?))
     (CljAvroSerializer. serializer schema))))


(deftype CljAvroDeserializer [^Deserializer deserializer schema]
  Deserializer
  (close [this]
    (.close deserializer))
  (configure [this configs key?]
    (.configure deserializer configs key?))
  (deserialize [this topic msg]
    (let [v (.deserialize deserializer topic msg)]
      (if (and (string? v)
               (avro-schema/uuid-schema? (avro-schema/parse-schema schema)))
        (uuid/as-uuid v)
        (avro-schema/generic-record->map v)))))

(defn avro-deserializer
  "Makes an avro deserializer"
  ([schema config key?]
   (avro-deserializer nil schema config key?))
  ([registry-client schema {:keys [schema-registry-url] :as base-config} key?]
   (let [deserializer (KafkaAvroDeserializer. registry-client)]
     (let [config (java.util.HashMap.)]
       (.put config "schema.registry.url" (registry/url base-config))
       (.configure deserializer config key?))
     (CljAvroDeserializer. deserializer schema))))

(defn avro-serde
  "Creates an avro serde.

   Defaults to using the cached schema registry client but this default
   can be overridden by setting :schema.registry/client to something
   else."
  ([config json-schema key?]
   (Serdes/serdeFrom (avro-serializer json-schema config key?)
                     (avro-deserializer json-schema config key?)))
  ([{json-schema :avro/schema
     schema-registry-url "schema.registry.url"
     registry-client :schema.registry/client
     :or {registry-client (registry/client config 100)}
     :as config}
    key?]
   (Serdes/serdeFrom (avro-serializer registry-client json-schema config key?)
                     (avro-deserializer registry-client json-schema config key?))))
