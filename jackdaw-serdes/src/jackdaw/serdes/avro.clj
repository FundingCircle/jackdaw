(ns jackdaw.serdes.avro
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [jackdaw.serdes.registry :as registry]
            [jackdaw.serdes.fn :as fn])
  (:import (io.confluent.kafka.serializers KafkaAvroSerializer KafkaAvroDeserializer)
           (java.lang CharSequence)
           (java.nio ByteBuffer)
           (java.util Collection Map UUID)
           (org.apache.avro Schema$Parser Schema$ArraySchema Schema Schema$Field)
           (org.apache.avro.generic GenericData$Array GenericData$EnumSymbol GenericData$Record GenericRecordBuilder)
           (org.apache.kafka.common.serialization Serializer Deserializer Serdes)))

(set! *warn-on-reflection* true)
;; Private Helpers

(def parse-schema-str
  (memoize (fn [schema-str]
             (when schema-str
               (.parse (Schema$Parser.) ^String schema-str)))))

(defn- ^String mangle [^String n]
  (str/replace n #"-" "_"))

(defn- ^String unmangle [^String n]
  (str/replace n #"_" "-"))

(defn- dispatch-on-type-fields [^Schema schema]
  (when schema
    (let [base-type (-> schema (.getType) (.getName))
          logical-type (-> schema (.getProps) (.get "logicalType"))]
      (if logical-type
        {:type base-type :logical-type logical-type}
        {:type base-type}))))

;; Protocols and Multimethods

(defprotocol SchemaType
  (match-clj? [schema-type clj-data])
  (match-avro? [schema-type avro-data])
  (avro->clj [schema-type avro-data])
  (clj->avro [schema-type clj-data path]))

(defmulti schema-type dispatch-on-type-fields)

;; Validation

(defn class-name
  "Returns a human readable description of x's type"
  [x]
  ;; nil does not have a class
  (if x
    (.getCanonicalName (class x))
    "nil"))

(defn serialization-error-msg [x expected-type]
  (format "%s is not a valid type for %s"
          (class-name x)
          expected-type))

(defn validate-clj! [this x path expected-type]
  (when-not (match-clj? this x)
    (throw (ex-info (serialization-error-msg x expected-type)
                    {:path path
                     :data x}))))

;; Primitive Types

;;; Boolean

(defrecord BooleanType []
  SchemaType
  (match-clj? [_ x] (boolean? x))
  (match-avro? [_ x] (boolean? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "bool")
    x))

(defmethod schema-type {:type "boolean"} [_]
  (BooleanType.))

;;; Bytes

(defn- byte-buffer?
  [x]
  (instance? ByteBuffer x))

(def avro-bytes?
  "Returns true if the object is compatible with Avro bytes, false otherwise

  * byte[] - Valid only as a top level schema type
  * java.nio.ByteBuffer - Valid only as a nested type"
  (some-fn byte-buffer? bytes?))

(defrecord BytesType []
  SchemaType
  (match-clj? [_ x] (avro-bytes? x))
  (match-avro? [_ x] (avro-bytes? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "bytes")
    x))

(defmethod schema-type {:type "bytes"} [_]
  (BytesType.))

;;; Double

;; Note that clojure.core/float? recognizes both single and double precision floating point values.

(defrecord DoubleType []
  SchemaType
  (match-clj? [_ x] (float? x))
  (match-avro? [_ x] (float? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "double")
    x))

(defmethod schema-type {:type "double"} [_]
  (DoubleType.))

;;; Float

(defn single-float? [x]
  (instance? Float x))

(defrecord FloatType []
  SchemaType
  (match-clj? [_ x] (single-float? x))
  (match-avro? [_ x] (single-float? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "float")
    x))

(defmethod schema-type {:type "float"} [_]
  (FloatType.))

;;; Int

(defn int-range? [x]
  (<= Integer/MIN_VALUE x Integer/MAX_VALUE))

(defn int-castable? [x]
  (and (int? x)
       (int-range? x)))

(defrecord IntType []
  SchemaType
  (match-clj? [_ x]
    (int-castable? x))
  (match-avro? [_ x] (int? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "int")
    (int x)))

(defmethod schema-type {:type "int"} [_]
  (IntType.))

;;; Long

(defrecord LongType []
  SchemaType
  (match-clj? [_ x]
    (int? x))
  (match-avro? [_ x] (int? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "long")
    (long x)))

(defmethod schema-type {:type "long"} [_]
  (LongType.))

;;; String

(defrecord StringType []
  SchemaType
  (match-clj? [_ x] (string? x))
  (match-avro? [_ x] (instance? CharSequence x))
  (avro->clj [_ x] (str x))
  (clj->avro [this x path]
    (validate-clj! this x path "string")
    x))

(defmethod schema-type {:type "string"} [_]
  (StringType.))

;;; Null

(defrecord NullType []
  SchemaType
  (match-clj? [_ x] (nil? x))
  (match-avro? [_ x] (nil? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "nil")
    x))

(defmethod schema-type {:type "null"} [_]
  (NullType.))

;; Schemaless

(defrecord SchemalessType []
  SchemaType
  (match-clj? [_ x]
    true)
  (match-avro? [_ x]
    true)
  (avro->clj [_ x] x)
  (clj->avro [_ x path] x))

(defmethod schema-type nil [_]
  (SchemalessType.))

;; Complex Types

;;; Array

(defrecord ArrayType [^Schema schema]
  SchemaType
  (match-clj? [_ x]
    (sequential? x))
  (match-avro? [_ x]
    (instance? GenericData$Array x))
  (avro->clj [_ java-collection]
    (let [element-type (.getElementType ^Schema$ArraySchema (.getSchema ^GenericData$Array java-collection))
          element-schema (schema-type element-type)]
      (mapv #(avro->clj element-schema %) java-collection)))
  (clj->avro [this clj-seq path]
    (validate-clj! this clj-seq path "array")

    (let [element-type (.getElementType ^Schema$ArraySchema schema)
          element-schema (schema-type element-type)]

      (GenericData$Array. ^Schema schema
                          ^Collection
                          (map-indexed (fn [i x]
                                         (clj->avro element-schema x (conj path i)))
                                       clj-seq)))))

(defmethod schema-type {:type "array"} [schema]
  (ArrayType. schema))

;;; Enum

(defrecord EnumType [^Schema schema]
  SchemaType
  (match-clj? [_ x]
    (or
      (string? x)
      (keyword? x)))
  (match-avro? [_ x]
    (instance? GenericData$EnumSymbol x))
  (avro->clj [_ avro-enum]
    (-> (.toString avro-enum)
        (unmangle)
        (keyword)))
  (clj->avro [this clj-keyword path]
    (validate-clj! this clj-keyword path "enum")

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
  (clj->avro [_ fixed path]
    (throw (UnsupportedOperationException. "Not implemented"))))

(defmethod schema-type {:type "fixed"} [_]
  (FixedType.))

;;; Map

(defrecord MapType [^Schema schema]
  SchemaType
  (match-clj? [_ x]
    (map? x))
  (match-avro? [_ x]
    (instance? Map x))
  (avro->clj [_ avro-map]
    (let [value-type (.getValueType schema)
          value-schema (schema-type value-type)]
      (->> (for [[k v] avro-map]
             [(str k) (avro->clj value-schema v)])
           (into {}))))
  (clj->avro [this clj-map path]
    (validate-clj! this clj-map path "map")

    (let [value-type (.getValueType schema)
          value-schema (schema-type value-type)]
      (reduce-kv (fn [acc k v]
                   (when-not (string? k)
                     (throw (ex-info (format "%s (%s) is not a valid map key type, only string keys are supported"
                                             (class-name k)
                                             k)
                                     {:path path
                                      :clj-data clj-map})))

                   (let [new-v (clj->avro value-schema v (conj path k))]
                     (assoc acc k new-v)))
                 {}
                 clj-map))))

(defmethod schema-type {:type "map"} [schema]
  (MapType. schema))

;;; Record

(defrecord RecordType [^Schema schema]
  SchemaType
  (match-clj? [_ clj-map]
    (let [fields (.getFields schema)]
      (every? (fn [^Schema$Field field]
                (let [field-schema-type (schema-type (.schema field))
                      field-value (get clj-map (keyword (unmangle (.name field))) ::missing)]

                  (if (= field-value ::missing)
                    (.defaultValue field)
                    (match-clj? field-schema-type field-value))))
              fields)))
  (match-avro? [_ avro-record]
    (or (instance? GenericData$Record avro-record)
        (nil? avro-record)))
  (avro->clj [_ avro-record]
    (when avro-record
      (let [avro-record ^GenericData$Record avro-record
            record-schema (.getSchema ^GenericData$Record avro-record)]
        (->> (for [^Schema$Field field (.getFields record-schema)
                   :let [field-name (.name field)
                         field-schema (schema-type (.schema field))
                         value (.get avro-record field-name)]]
               [(keyword (unmangle field-name)) (avro->clj field-schema value)])
             (into {})))))
  (clj->avro [_ clj-map path]
    (when-not (map? clj-map)
      (throw (ex-info (serialization-error-msg clj-map "record") {:path path
                                                                  :clj-data clj-map})))

    (let [record-builder (GenericRecordBuilder. schema)]

      (doseq [[k v] clj-map]
        (let [new-k (mangle (name k))
              field (.getField schema new-k)
              _ (when-not field
                  (throw (ex-info (format "Field %s not known in %s"
                                          new-k
                                          (.getName schema))
                                  {:path path
                                   :clj-data clj-map})))
              child-schema (schema-type (.schema field))
              new-v (clj->avro child-schema v (conj path k))]
          (.set record-builder new-k new-v)))

      (try
        (.build record-builder)
        (catch org.apache.avro.AvroRuntimeException e
          (throw (ex-info (str (.getMessage e)) {:path path :clj-data clj-map} e)))))))

(defmethod schema-type {:type "record"} [schema]
  (RecordType. schema))

;;; Union

(defn- union-types [^Schema union-schema]
  (->> (.getTypes union-schema)
       (map schema-type)
       (into #{})))

(defn union-types-str [^Schema union-schema]
  (->> (.getTypes union-schema)
       (map (fn [^Schema s]
              (.getType s)))
       (str/join ", ")))

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
  (clj->avro [_ clj-data path]
    (let [schema-type (match-union-type schema #(match-clj? % clj-data))]
      (when-not schema-type
        (throw (ex-info (serialization-error-msg clj-data
                                                 (format "union [%s]" (union-types-str schema)))
                        {:path path
                         :clj-data clj-data})))

      (clj->avro schema-type clj-data path))))

(defmethod schema-type {:type "union"} [schema]
  (UnionType. schema))

;; Serde Factory

(defn- base-config [registry-url]
  {"schema.registry.url" registry-url})

(defn- avro-serializer [serde-config]
  (let [{:keys [registry-client registry-url avro-schema key?]} serde-config
        base-serializer (KafkaAvroSerializer. registry-client)
        methods {:close (fn [_]
                          (.close base-serializer))
                 :configure (fn [_ base-config key?]
                              (.configure base-serializer base-config key?))
                 :serialize (fn [_ topic data]
                              (let [schema-type (schema-type avro-schema)]
                                (try
                                  (.serialize base-serializer topic (clj->avro schema-type data []))
                                  (catch clojure.lang.ExceptionInfo e
                                    (let [data (-> e
                                                   ex-data
                                                   (assoc :topic topic :clj-data data))]
                                      (throw (ex-info (.getMessage e) data)))))))}

        clj-serializer (fn/new-serializer methods)]
    (.configure clj-serializer (base-config registry-url) key?)
    clj-serializer))

(defrecord StringUUIDType []
  SchemaType
  (match-clj? [_ uuid]
    (uuid? uuid))
  (match-avro? [_ uuid-str]
    (instance? CharSequence uuid-str))
  (avro->clj [_ uuid-utf8]
    (try
      (UUID/fromString (str uuid-utf8))
      (catch Exception e
        (str uuid-utf8))))
  (clj->avro [this uuid path]
    (validate-clj! this uuid path "uuid")
    (str uuid)))

(defmethod schema-type
  {:type "string" :logical-type "jackdaw.serdes.avro.UUID"}
  [_]
  (StringUUIDType.))

(defmethod schema-type
  {:type "string" :logical-type "uuid"}
  [_]
  (StringUUIDType.))

(defn- avro-deserializer [serde-config]
  (let [{:keys [registry-client registry-url avro-schema key?]} serde-config
        base-deserializer (KafkaAvroDeserializer. registry-client)
        methods {:close (fn [_]
                          (.close base-deserializer))
                 :configure (fn [_ base-config key?]
                              (.configure base-deserializer base-config key?))
                 :deserialize (fn [_ topic raw-data]
                                (try
                                  (let [schema-type (schema-type avro-schema)
                                        avro-data (.deserialize base-deserializer topic raw-data)]
                                    (assert (match-avro? schema-type avro-data))
                                    (avro->clj schema-type avro-data))
                                  (catch Exception e
                                    (let [msg "Deserialization error"]
                                      (log/error (str msg " for " topic))
                                      (throw (ex-info msg {:topic topic} e))))))}

        clj-deserializer (fn/new-deserializer methods)]
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
