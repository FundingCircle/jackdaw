(ns jackdaw.serdes.avro
  (:refer-clojure :exclude [boolean? bytes?])
  (:require [jackdaw.serdes.registry :as registry])
  (:import (org.apache.kafka.common.serialization Serdes Serializer Deserializer)
           (java.util UUID Map HashMap)
           (org.apache.avro.generic GenericData$Record GenericData$Array GenericData$EnumSymbol)
           (org.apache.avro Schema$ArraySchema Schema Schema$Parser)
           (io.confluent.kafka.serializers KafkaAvroSerializer KafkaAvroDeserializer)
           (io.confluent.kafka.schemaregistry.client SchemaRegistryClient)))

;; Private Helpers

(def parse-schema-str
  (memoize (fn [schema-str]
             (.parse (Schema$Parser.) ^String schema-str))))

(defn- mangle [^String n]
  (clojure.string/replace n #"-" "_"))

(defn- unmangle [^String n]
  (clojure.string/replace n #"_" "-"))

(defn- dispatch-on-type-fields [schema]
  (let [base-type (-> schema (.getType) (.getName))
        logical-type (-> schema (.getProps) (.get "logicalType"))]
    (if logical-type
      {:type base-type :logical-type logical-type}
      {:type base-type})))

;; Protocols and Multimethods

(defprotocol SchemaType
  (match-clj? [schema-type clj-data])
  (match-avro? [schema-type avro-data])
  (avro->clj [schema-type avro-data])
  (clj->avro [schema-type clj-data]))

(defmulti schema-type dispatch-on-type-fields)

;; Primitive Types

;;; Boolean

(defn- boolean?
  "Return true if x is a Boolean"
  {:added "1.9"}
  [x] (instance? Boolean x))

(defrecord BooleanType []
  SchemaType
  (match-clj? [_ x] (boolean? x))
  (match-avro? [_ x] (boolean? x))
  (avro->clj [_ x] x)
  (clj->avro [_ x] x))

(defmethod schema-type {:type "boolean"} [_]
  (BooleanType.))

;;; Bytes

(defn- bytes?
  "Return true if x is a byte array"
  {:added "1.9"}
  [x] (if (nil? x)
        false
        (-> x class .getComponentType (= Byte/TYPE))))

(defrecord BytesType []
  SchemaType
  (match-clj? [_ x] (bytes? x))
  (match-avro? [_ x] (bytes? x))
  (avro->clj [_ x] x)
  (clj->avro [_ x] x))

(defmethod schema-type {:type "bytes"} [_]
  (BytesType.))

;;; Double

(defrecord DoubleType []
  SchemaType
  (match-clj? [_ x] (instance? Double x))
  (match-avro? [_ x] (instance? Double x))
  (avro->clj [_ x] x)
  (clj->avro [_ x] x))

(defmethod schema-type {:type "double"} [_]
  (DoubleType.))

;;; Float

(defrecord FloatType []
  SchemaType
  (match-clj? [_ x] (instance? Float x))
  (match-avro? [_ x] (instance? Float x))
  (avro->clj [_ x] x)
  (clj->avro [_ x] x))

(defmethod schema-type {:type "float"} [_]
  (FloatType.))

;;; Int

(defrecord IntType []
  SchemaType
  (match-clj? [_ x] (integer? x))
  (match-avro? [_ x] (integer? x))
  (avro->clj [_ x] x)
  (clj->avro [_ x] x))

(defmethod schema-type {:type "int"} [_]
  (IntType.))

;;; Long

(defrecord LongType []
  SchemaType
  (match-clj? [_ x] (instance? Long x))
  (match-avro? [_ x] (instance? Long x))
  (avro->clj [_ x] x)
  (clj->avro [_ x] x))

(defmethod schema-type {:type "long"} [_]
  (LongType.))

;;; String

(defrecord StringType []
  SchemaType
  (match-clj? [_ x] (string? x))
  (match-avro? [_ x] (string? x))
  (avro->clj [_ x] (str x))
  (clj->avro [_ x] x))

(defmethod schema-type {:type "string"} [_]
  (StringType.))

;;; Null

(defrecord NullType []
  SchemaType
  (match-clj? [_ x] (nil? x))
  (match-avro? [_ x] (nil? x))
  (avro->clj [_ x] x)
  (clj->avro [_ x] x))

(defmethod schema-type {:type "null"} [_]
  (NullType.))

;; Complex Types

;;; Array

(defrecord ArrayType [schema]
  SchemaType
  (match-clj? [_ x]
    (seq? x))
  (match-avro? [_ x]
    (instance? GenericData$Array x))
  (avro->clj [_ java-collection]
    (let [element-type (.getElementType ^Schema$ArraySchema schema)
          element-schema (schema-type element-type)]
      (mapv #(avro->clj element-schema %) java-collection)))
  (clj->avro [_ clj-seq]
    clj-seq))

(defmethod schema-type {:type "array"} [schema]
  (ArrayType. schema))

;;; Enum

(defrecord EnumType [schema]
  SchemaType
  (match-clj? [_ x]
    (keyword? x))
  (match-avro? [_ x]
    (instance? GenericData$EnumSymbol x))
  (avro->clj [_ avro-enum]
    (-> (.toString avro-enum)
        (unmangle)
        (keyword)))
  (clj->avro [_ clj-keyword]
    (->> (name clj-keyword)
         (mangle)
         (GenericData$EnumSymbol. schema))))

(defmethod schema-type {:type "enum"} [schema]
  (EnumType. schema))

;;; Fixed

(defrecord FixedType []
  SchemaType
  (match-clj? [_ x]
    false)
  (match-avro? [_ x]
    false)
  (avro->clj [_ fixed]
    (throw (UnsupportedOperationException. "Not implemented")))
  (clj->avro [_ fixed]
    (throw (UnsupportedOperationException. "Not implemented"))))

(defmethod schema-type {:type "fixed"} [_]
  (FixedType.))

;;; Map

(defrecord MapType [schema]
  SchemaType
  (match-clj? [_ x]
    (map? x))
  (match-avro? [_ x]
    (instance? Map x))
  (avro->clj [_ avro-map]
    (->> (for [[k v] avro-map]
           [(keyword k) v])
         (into {})))
  (clj->avro [_ clj-map]
    (reduce-kv (fn [acc k v]
                 (let [value-type (.getValueType schema)
                       value-schema (schema-type value-type)
                       new-k (mangle (name k))
                       new-v (clj->avro value-schema v)]
                   (assoc acc new-k new-v)))
               {}
               clj-map)))

(defmethod schema-type {:type "map"} [schema]
  (MapType. schema))

;;; Record

(defrecord RecordType [schema]
  SchemaType
  (match-clj? [_ clj-map]
    (let [fields (.getFields schema)]
      (and
       (= (count fields) (count clj-map))
       (every? (fn [field]
                 (let [field-schema-type (schema-type (.schema field))
                       field-value (get clj-map (keyword (unmangle (.name field))))]
                   (match-clj? field-schema-type field-value)))
               fields))))
  (match-avro? [_ avro-record]
    (instance? GenericData$Record avro-record))
  (avro->clj [_ avro-record]
    (->> (for [field (.getFields schema)
               :let [name (.name field)
                     schema (schema-type (.schema field))
                     value (.get avro-record name)]]
           [(keyword (unmangle name)) (avro->clj schema value)])
         (into {})))
  (clj->avro [_ clj-map]
    (reduce-kv (fn [acc k v]
                 (let [new-k (mangle (name k))
                       field (.getField schema new-k)
                       _ (assert field (format "Field %s not known in %s"
                                               new-k
                                               (.getName schema)))
                       child-schema (schema-type (.schema field))
                       new-v (clj->avro child-schema v)]
                   (.put acc new-k new-v)
                   acc))
               (GenericData$Record. schema)
               clj-map)))

(defmethod schema-type {:type "record"} [schema]
  (RecordType. schema))

;;; Union

(defn- union-types [union-schema]
  (->> (.getTypes union-schema)
       (map schema-type)
       (into #{})))

(defn- match-union-type [schema pred]
  (some #(when (pred %) %) (union-types schema)))

(defrecord UnionType [schema]
  SchemaType
  (match-clj? [_ clj-data]
    (boolean (match-union-type schema #(match-clj? % clj-data))))
  (match-avro? [_ avro-data]
    (boolean (match-union-type schema #(match-avro? % avro-data))))
  (avro->clj [_ avro-data]
    (let [schema-type (match-union-type schema #(match-avro? % avro-data))]
      (avro->clj schema-type avro-data)))
  (clj->avro [_ clj-data]
    (let [schema-type (match-union-type schema #(match-clj? % clj-data))]
      (if-not schema-type
        (throw (ex-info (str "No matching union schema")
                        {:schema schema :value clj-data}))
        (clj->avro schema-type clj-data)))))

(defmethod schema-type {:type "union"} [schema]
  (UnionType. schema))

;; Serde Factory

(deftype CljSerializer [base-serializer avro-schema]
  Serializer
  (close [_]
    (.close base-serializer))
  (configure [_ base-config key?]
    (.configure base-serializer base-config key?))
  (serialize [_ topic clj-data]
    (let [schema-type (schema-type avro-schema)]
      (assert (match-clj? schema-type clj-data))
      (.serialize base-serializer topic (clj->avro schema-type clj-data)))))

(defn- base-config [registry-url]
  {"schema.registry.url" registry-url})

(defn- avro-serializer [serde-config]
  (let [{:keys [registry-client registry-url avro-schema key?]} serde-config
        base-serializer (KafkaAvroSerializer. registry-client)
        clj-serializer (CljSerializer. base-serializer avro-schema)]
    (.configure clj-serializer (base-config registry-url) key?)
    clj-serializer))

(deftype CljDeserializer [base-deserializer avro-schema]
  Deserializer
  (close [_]
    (.close base-deserializer))
  (configure [_ base-config key?]
    (.configure base-deserializer base-config key?))
  (deserialize [_ topic raw-data]
    (let [schema-type (schema-type avro-schema)
          avro-data (.deserialize base-deserializer topic raw-data)]
      (assert (match-avro? schema-type avro-data))
      (avro->clj schema-type avro-data))))

(defn- avro-deserializer [serde-config]
  (let [{:keys [registry-client registry-url avro-schema key?]} serde-config
        base-deserializer (KafkaAvroDeserializer. registry-client)
        clj-deserializer (CljDeserializer. base-deserializer avro-schema)]
    (.configure clj-deserializer (base-config registry-url) key?)
    clj-deserializer))

;; Public API

(defn avro-serde [serde-config]
  (let [serializer (avro-serializer serde-config)
        deserializer (avro-deserializer serde-config)]
    (Serdes/serdeFrom serializer deserializer)))

(defn serde-config [key-or-value topic-config]
  (let [{:keys [:schema.registry/client :schema.registry/url
                :avro/schema]} topic-config]
    {:avro-schema (parse-schema-str schema)
     :registry-client (or client (registry/client topic-config 10))
     :registry-url (or url (registry/url topic-config))
     :key? (case key-or-value
             :key true
             :value false)}))
