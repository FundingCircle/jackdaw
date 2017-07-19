(ns jackdaw.serdes.avro2
  (:require [jackdaw.serdes.avro2.impl :as impl]
            [clojure.future :refer [uuid? boolean? bytes? double?]])
  (:import (org.apache.kafka.common.serialization Serdes)
           (java.util UUID Map HashMap)
           (org.apache.avro.generic GenericData$Record GenericData$Array GenericData$EnumSymbol)
           (org.apache.avro Schema$ArraySchema Schema)))

(defprotocol SchemaType
  (match-clj? [schema-type clj-data])
  (match-avro? [schema-type avro-data])
  (avro->clj [schema-type avro-data])
  (clj->avro [schema-type clj-data]))

(defn- dispatch-on-type-fields [schema]
  (let [base-type (impl/base-type-name schema)
        logical-type (impl/logical-type-name schema)]
    (if logical-type
      {:type base-type :logical-type logical-type}
      {:type base-type})))

(defmulti schema-type dispatch-on-type-fields)

;; Primitive Types

;;; Boolean

(defrecord BooleanType []
  SchemaType
  (match-clj? [_ x] (boolean? x))
  (match-avro? [_ x] (boolean? x))
  (avro->clj [_ x] x)
  (clj->avro [_ x] x))

(defmethod schema-type {:type "boolean"} [_]
  (BooleanType.))

;;; Bytes

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
  (avro->clj [_ x] x)
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
        (keyword)))
  (clj->avro [_ clj-keyword]
    (->> (name clj-keyword)
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
                       new-k (name k)
                       new-v (clj->avro value-schema v)]
                   (.put acc new-k new-v)
                   acc))
               (HashMap.)
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
                       field-value (get clj-map (keyword (.name field)))]
                   (match-clj? field-schema-type field-value)))
               fields))))
  (match-avro? [_ avro-record]
    (instance? GenericData$Record avro-record))
  (avro->clj [_ avro-record]
    (->> (for [field (.getFields schema)
               :let [name (.name field)
                     schema (schema-type (.schema field))
                     value (.get avro-record name)]]
           [(keyword name) (avro->clj schema value)])
         (into {})))
  (clj->avro [_ clj-map]
    (reduce-kv (fn [acc k v]
                 (let [new-k (name k)
                       child-schema (-> (.getField schema new-k)
                                        (.schema)
                                        (schema-type))
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
      (clj->avro schema-type clj-data))))

(defmethod schema-type {:type "union"} [schema]
  (UnionType. schema))

;; Serde Factory

(defn avro-serde [{:keys [key? schema-str] :as config}]
  (let [schema (impl/parse-schema-str schema-str)
        base-opts {:key? (or key? false)
                   :registry-client nil
                   :base-config {"schema.registry.url" nil}}
        schema-type (schema-type schema config)
        serializer (impl/new-serializer
                    (assoc base-opts :convert-fn #(do
                                                    (assert (match-clj? schema-type %))
                                                    (clj->avro schema-type %))))
        deserializer (impl/new-deserializer
                      (assoc base-opts :convert-fn #(do
                                                      (assert (match-avro? schema-type %))
                                                      (avro->clj schema-type %))))]
    (Serdes/serdeFrom serializer deserializer)))
