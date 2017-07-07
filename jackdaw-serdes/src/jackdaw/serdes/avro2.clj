(ns jackdaw.serdes.avro2
  (:require [jackdaw.serdes.avro2.impl :as impl]
            [clojure.future :refer [uuid? boolean? bytes? double?]])
  (:import (org.apache.kafka.common.serialization Serdes)
           (java.util UUID Map)
           (org.apache.avro.generic GenericData$Record GenericData$Array)))

(defprotocol SchemaType
  (avro->clj [schema-type avro-data])
  (clj->avro [schema-type clj-data]))

(defmulti schema-type (fn [schema _config]
                        (if-let [logical-type (impl/get-logical-type-name schema)]
                          {:logical-type logical-type}
                          {:type (impl/get-base-type-name schema)})))

(defn- primitive-type [matcher]
  (reify SchemaType
    (avro->clj [_ x] x)
    (clj->avro [_ x] x)))

(defmethod schema-type {:type "array"} [schema]
  (reify SchemaType
    (avro->clj [_ ^GenericData$Array avro-data]
      (mapv #(avro->clj (schema-type (.get (.getFields schema) "items")) %)
            (.toArray avro-data)))
    (clj->avro [_ clj-data] clj-data)))

(defmethod schema-type {:type "boolean"} [_]
  (primitive-type boolean?))

(defmethod schema-type {:type "bytes"} [_]
  (reify SchemaType
    (avro->clj [_ bytes] bytes)
    (clj->avro [_ bytes] bytes)))

(defmethod schema-type {:type "double"} [_]
  (primitive-type double?))

(defmethod schema-type {:type "enum"} [schema]
  (reify SchemaType
    (avro->clj [_ avro-enum] avro-enum)
    (clj->avro [_ clj-keyword] (name clj-keyword))))

(defmethod schema-type {:type "fixed"} [schema]
  (reify SchemaType
    (avro->clj [_ fixed] fixed)
    (clj->avro [_ fixed] fixed)))

(defmethod schema-type {:type "float"} [_]
  (primitive-type float?))

(defmethod schema-type {:type "int"} [_]
  (primitive-type integer?))

(defmethod schema-type {:type "long"} [_]
  (primitive-type number?))

(defmethod schema-type {:type "map"} [schema]
  (reify SchemaType
    (avro->clj [_ avro-map] avro-map)
    (clj->avro [_ clj-map]
      (impl/reduce-fields clj-map schema clj->avro (Map.)))))

(defmethod schema-type {:type "null"} [_]
  (primitive-type nil?))

(defmethod schema-type {:type "record"} [schema]
  (reify SchemaType
    (avro->clj [_ avro-data])
    (clj->avro [_ clj-data]
      (let [init (GenericData$Record. schema)]
        (impl/reduce-fields clj-data schema clj->avro init)))))

(defmethod schema-type {:type "string"} [_]
  (primitive-type string?))

(defmethod schema-type {:type "union"} [schema]
  (primitive-type (constantly true)))

(defmethod schema-type {:logical-type "jackdaw.serdes.avro.UUID"} [_]
  (reify SchemaType
    (avro->clj [_ uuid-str] (UUID/fromString uuid-str))
    (clj->avro [_ uuid] (str uuid))))

(defn avro-serde [{:keys [key? schema-str] :as config}]
  (let [schema (impl/parse-schema-str schema-str)
        base-opts {:key? (or key? false)
                   :registry-client nil
                   :base-config {"schema.registry.url" nil}}
        schema-type (schema-type schema config)
        serializer (impl/new-serializer
                    (assoc base-opts :convert-fn #(avro->clj schema-type %)))
        deserializer (impl/new-deserializer
                      (assoc base-opts :convert-fn #(clj->avro schema-type %)))]
    (Serdes/serdeFrom serializer deserializer)))
