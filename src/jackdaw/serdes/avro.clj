(ns jackdaw.serdes.avro
  "Generating Serdes mapping Clojure <-> Avro.

  The intentional API of this NS has three main features -
  `SchemaCoercion`, the intentional type registry (of which
  `#'+base-schema-type-registry+` is an example) and
  `#'avro-serde`.

  `avro-serde` is the primary entry point to this namespace for
  users. It's a function of a schema-registry configuration, a schema
  type registry, and a serde configuration to be instantiated.

  The intent is that an end user will `partial` the `avro-serde`
  function with their schema registry details and desired type
  registry, and use the `partial`'d function as en entry in a registry
  as used by `jackdaw.serdes/serde`.

  This allows `serdes` and `avro-serde` to be agnostic to application
  or environment specific configuration details.

  But what's this type registry?

  Apache Avro \"logical types\" - a tool for annotating fields in an
  avro record as having some complex interpretation beyond their
  serialized format. The `type-registry` for the purposes of the
  `avro-serde` function a mapping of addresses to functions which will
  when invoked build and return a `SchemaCoercion` instance.

  When a Serde is instantiated, a stack of `SchemaCoercion` coersion
  helpers is built which will - given a simply deserialized Avro
  record - walk its tree coercing its to Clojure types as defined by
  the `SchemaCoercion` helpers.

  The `SchemaCoercion` stack is built by statically inspecting the parsed
  Avro schema, and using the type (if any) and potentially logical
  type to select a handler in the `type-registry` which will, given a
  function with which to recurse and the schema of that node, build
  and return a `SchemaCoercion` handler.

  This registry pattern is deliberately chosen so that Avro coercion
  will be customizable by the user. As an example, the
  `+UUID-type-registry+` is included, which defines a mapping from two
  different logical UUID refinements of the binary string type to an
  appropriate handler.

  A user who wanted to opt into these handlers could simply call
  `avro-serde` with
  `(merge +base-schema-type-registry+ +UUID-type-registry+)`

  Users are HIGHLY encouraged to use the `+base-schema-type-registry+`
  as the base for their type registries, as it defines sane handlings
  for all of Avro's fundamental types and most of its compounds.

  "
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.tools.logging :as log]
            [clojure.core.cache :as cache]
            [clojure.string :as str]
            [jackdaw.serdes.avro.schema-registry :as registry]
            [jackdaw.serdes.fn :as fn])
  (:import [io.confluent.kafka.serializers
            KafkaAvroSerializer KafkaAvroDeserializer]
           java.lang.CharSequence
           java.nio.ByteBuffer
           [java.util Collection Map UUID]
           [org.apache.avro
            Schema$Parser Schema$ArraySchema Schema Schema$Field]
           [org.apache.avro.generic
            GenericContainer GenericData$Array GenericData$EnumSymbol
            GenericData$Record GenericRecordBuilder]
           [org.apache.kafka.common.serialization
            Serializer Deserializer Serdes]))

(set! *warn-on-reflection* true)

;; Private Helpers

(def parse-schema-str
  (memoize
   (fn [schema-str]
     (when schema-str
       (.parse (Schema$Parser.) ^String schema-str)))))

(defn- ^String mangle [^String n]
  (str/replace n #"-" "_"))

(defn- ^String unmangle [^String n]
  (str/replace n #"_" "-"))

(defn- dispatch-on-type-fields
  [^Schema schema]
  (when schema
    (let [base-type (-> schema (.getType) (.getName))
          logical-type (-> schema (.getProps) (.get "logicalType"))]
      (if logical-type
        {:type base-type :logical-type logical-type}
        {:type base-type}))))

(defn make-coercion-stack
  "Given a registry mapping Avro type specs to 2-arity coercion
  constructors, recursively build up a coercion stack which will go
  clj <-> avro, returning the root coercion object.

  (satisfies `SchemaCoercion`)"
  [type-registry]
  (fn stack [^Schema schema]
    (let [dispatch (dispatch-on-type-fields schema)
          ctor (or (get type-registry dispatch)
                   (when (contains? dispatch :logical-type)
                     (get type-registry (dissoc dispatch :logical-type))))]
      (if-not ctor
        (throw (ex-info "Failed to dispatch coersion!"
                        {:schema schema, :dispatch dispatch}))
        (ctor stack schema)))))

;; Protocols and Multimethods

(defprotocol SchemaCoercion
  (match-clj? [schema-type clj-data])
  (match-avro? [schema-type avro-data])
  (avro->clj [schema-type avro-data])
  (clj->avro [schema-type clj-data path]))

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
                    {:path path, :data x}))))

;;;; Primitive Types

;;; Boolean

(defrecord BooleanType []
  SchemaCoercion
  (match-clj? [_ x] (boolean? x))
  (match-avro? [_ x] (boolean? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "bool")
    x))

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
  SchemaCoercion
  (match-clj? [_ x] (avro-bytes? x))
  (match-avro? [_ x] (avro-bytes? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "bytes")
    x))

;;; Double

;; Note that clojure.core/float? recognizes both single and double precision floating point values.

(defrecord DoubleType []
  SchemaCoercion
  (match-clj? [_ x] (float? x))
  (match-avro? [_ x] (float? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "double")
    x))

(defn single-float? [x]
  (instance? Float x))

(defrecord FloatType []
  SchemaCoercion
  (match-clj? [_ x] (single-float? x))
  (match-avro? [_ x] (single-float? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "float")
    x))

(defn int-range? [x]
  (<= Integer/MIN_VALUE x Integer/MAX_VALUE))

(defn int-castable? [x]
  (and (int? x)
       (int-range? x)))

(defrecord IntType []
  SchemaCoercion
  (match-clj? [_ x]
    (int-castable? x))
  (match-avro? [_ x] (int? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "int")
    (int x)))

(defrecord LongType []
  SchemaCoercion
  (match-clj? [_ x]
    (int? x))
  (match-avro? [_ x] (int? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "long")
    (long x)))

(defrecord StringType []
  SchemaCoercion
  (match-clj? [_ x] (string? x))
  (match-avro? [_ x] (instance? CharSequence x))
  (avro->clj [_ x] (str x))
  (clj->avro [this x path]
    (validate-clj! this x path "string")
    x))

(defrecord NullType []
  SchemaCoercion
  (match-clj? [_ x] (nil? x))
  (match-avro? [_ x] (nil? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "nil")
    x))

(defrecord SchemalessType []
  SchemaCoercion
  (match-clj? [_ x]
    true)
  (match-avro? [_ x]
    true)
  (avro->clj [_ x] x)
  (clj->avro [_ x path] x))

;; UUID :disapprove:

(defrecord StringUUIDType []
  SchemaCoercion
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

;;;; Complex Types

(defrecord ArrayType [^Schema schema element-coercion]
  SchemaCoercion
  (match-clj? [_ x]
    (sequential? x))

  (match-avro? [_ x]
    (instance? GenericData$Array x))

  (avro->clj [_ java-collection]
    (mapv #(avro->clj element-coercion %) java-collection))

  (clj->avro [this clj-seq path]
    (validate-clj! this clj-seq path "array")

    (GenericData$Array. ^Schema schema
                        ^Collection
                        (map-indexed (fn [i x]
                                       (clj->avro element-coercion x (conj path i)))
                                     clj-seq))))

(defn ->ArrayType
  "Wrapper by which to construct a `ArrayType` which handles the
  structural recursion of building the handler stack so that the
  `ArrayType` type can be pretty simple."
  [schema->coercion ^Schema schema]
  (ArrayType. schema
              (schema->coercion
               (.getElementType ^Schema$ArraySchema schema))))

(defrecord EnumType [^Schema schema]
  SchemaCoercion
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

(defn ->EnumType [_schema->coercion ^Schema schema]
  (EnumType. schema))

#_
(defrecord FixedType []
  SchemaCoercion
  (match-clj? [_ x]
    false)
  (match-avro? [_ x]
    false)
  (avro->clj [_ fixed]
    (throw (UnsupportedOperationException. "Not implemented")))
  (clj->avro [_ fixed path]
    (throw (UnsupportedOperationException. "Not implemented"))))

(defrecord MapType [^Schema schema value-coercion]
  SchemaCoercion
  (match-clj? [_ x]
    (map? x))

  (match-avro? [_ x]
    (instance? Map x))

  (avro->clj [_ avro-map]
    (into {}
          (map (fn [[k v]] [(str k) (avro->clj value-coercion v)]))
          avro-map))

  (clj->avro [this clj-map path]
    (validate-clj! this clj-map path "map")

    (into {}
          (map (fn [[k v]]
                 (when-not (string? k)
                   (throw (ex-info (format "%s (%s) is not a valid map key type, only string keys are supported"
                                           (class-name k)
                                           k)
                                   {:path path, :clj-data clj-map})))
                 [k (clj->avro value-coercion v (conj path k))]))
          clj-map)))

(defn ->MapType
  "Wrapper by which to construct a `MapType` which handles the
  structural recursion of building the handler stack so that the
  `MapType` type can be pretty simple."
  [schema->coercion ^Schema schema]
  (MapType. schema (schema->coercion (.getValueType schema))))

(defrecord RecordType [^Schema schema field->schema+coercion]
  SchemaCoercion
  (match-clj? [_ clj-map]
    (let [fields (.getFields schema)]
      (every? (fn [[field-key [^Schema$Field field field-coercion]]]
                (let [field-value (get clj-map field-key ::missing)]
                  (if (= field-value ::missing)
                    (.defaultValue field)
                    (match-clj? field-coercion field-value))))
              field->schema+coercion)))

  (match-avro? [_ avro-record]
    (or (instance? GenericData$Record avro-record)
        (nil? avro-record)))

  (avro->clj [_ avro-record]
    (when avro-record
      (into {}
            (comp (map first)
                  (map (fn [^Schema$Field field]
                         (let [field-name (.name field)
                               field-key (keyword (unmangle field-name))
                               [_ field-coercion :as entry] (get field->schema+coercion field-key)
                               value (.get ^GenericData$Record avro-record field-name)]
                           (when-not field-coercion
                             (throw (ex-info "Unable to deserialize field"
                                             {:field field
                                              :field-name field-name
                                              :field-key field-key
                                              :entry entry})))
                           [field-key (avro->clj field-coercion value)]))))
            (vals field->schema+coercion))))

  (clj->avro [_ clj-map path]
    (when-not (map? clj-map)
      (throw (ex-info (serialization-error-msg clj-map "record")
                      {:path path, :clj-data clj-map})))

    (let [record-builder (GenericRecordBuilder. schema)]
      (try
        (doseq [[k v] clj-map]
          (let [new-k (mangle (name k))
                field (.getField schema new-k)]
            (when-not field
              (throw (ex-info (format "Field %s not known in %s"
                                      new-k
                                      (.getName schema))
                              {:path path, :clj-data clj-map})))
            (let [[_ field-coercion] (get field->schema+coercion k)
                  new-v (clj->avro field-coercion v (conj path k))]
              (.set record-builder new-k new-v))))

        (.build record-builder)

        (catch org.apache.avro.AvroRuntimeException e
          (throw (ex-info (str (.getMessage e))
                          {:path path, :clj-data clj-map} e)))))))

(defn ->RecordType
  "Wrapper by which to construct a `RecordType` which handles the
  structural recursion of building the handler stack so that the
  `RecordType` type can be pretty simple."
  [schema->coercion ^Schema schema]
  (let [fields (into {}
                     (map (fn [^Schema$Field field]
                            [(keyword (unmangle (.name field)))
                             [field (schema->coercion (.schema field))]]))
                     (.getFields schema))]
    (RecordType. schema fields)))

(defn- match-union-type [coercion-types pred]
  (some #(when (pred %) %) coercion-types))

(defrecord UnionType [coercion-types schemas]
  SchemaCoercion
  (match-clj? [_ clj-data]
    (boolean (match-union-type coercion-types #(match-clj? % clj-data))))

  (match-avro? [_ avro-data]
    (boolean (match-union-type coercion-types #(match-avro? % avro-data))))

  (avro->clj [_ avro-data]
    (let [schema-type (match-union-type coercion-types #(match-avro? % avro-data))]
      (avro->clj schema-type avro-data)))

  (clj->avro [_ clj-data path]
    (if-let [schema-type (match-union-type coercion-types  #(match-clj? % clj-data))]
      (clj->avro schema-type clj-data path)
      (throw (ex-info (serialization-error-msg clj-data
                                               (->> schemas
                                                    (map #(.getType ^Schema %))
                                                    (str/join ", ")
                                                    (format "union [%s]")))
                      {:path path, :clj-data clj-data})))))

(defn ->UnionType
  "Wrapper by which to construct a `UnionType` which handles the
  structural recursion of building the handler stack so that the
  `UnionType` type can be pretty simple."
  [schema->coercion ^Schema schema]
  (let [schemas (->> (.getTypes schema)
                     (into []))
        coercions (->> schemas
                       (map schema->coercion)
                       (into []))]
    (UnionType. coercions schemas)))

;;;; Serde Factory

(defn- base-config [registry-url]
  {"schema.registry.url" registry-url})

(defn- serializer [schema->coercion serde-config]
  (let [{:keys [registry-client registry-url avro-schema key?]} serde-config
        base-serializer (KafkaAvroSerializer. registry-client)
        ;; This is invariant across subject schema changes, shockingly.
        coercion-type (schema->coercion avro-schema)
        methods {:close     (fn [_]
                              (.close base-serializer))
                 :configure (fn [_ base-config key?]
                              (.configure base-serializer base-config key?))
                 :serialize (fn [_ topic data]
                              (try
                                (.serialize base-serializer topic (clj->avro coercion-type data []))
                                (catch clojure.lang.ExceptionInfo e
                                  (let [data (-> e
                                                 ex-data
                                                 (assoc :topic topic :clj-data data))]
                                    (throw (ex-info (.getMessage e) data))))))}
        clj-serializer (fn/new-serializer methods)]
    (.configure clj-serializer (base-config registry-url) key?)
    clj-serializer))

(defn- deserializer [schema->coercion serde-config]
  (let [{:keys [registry-client registry-url avro-schema key?]} serde-config
        base-deserializer (KafkaAvroDeserializer. registry-client)
        methods {:close       (fn [_]
                                (.close base-deserializer))
                 :configure   (fn [_ base-config key?]
                                (.configure base-deserializer base-config key?))
                 :deserialize (fn [_ topic raw-data]
                                (try
                                  (let [avro-data (.deserialize base-deserializer topic raw-data)]
                                    ;; Note that `.deserialize` will return EITHER a Java Object, or
                                    ;; a ^GenericContainer. ^GenericContainer is only produced when
                                    ;; there was a schema associated with the deserialized data, and
                                    ;; only then do we use the coercion stack machinery.
                                    (if (instance? GenericContainer avro-data)
                                      (let [schema (.getSchema ^GenericContainer avro-data)
                                            coercion-type (schema->coercion schema)]
                                        (assert (match-avro? coercion-type avro-data))
                                        (avro->clj coercion-type avro-data))
                                      ;; Schemaless data can't have coercion
                                      avro-data))
                                  (catch Exception e
                                    (let [msg "Deserialization error"]
                                      (log/error e (str msg " for " topic))
                                      (throw (ex-info msg {:topic topic} e))))))}
        clj-deserializer (fn/new-deserializer methods)]
    (.configure clj-deserializer (base-config registry-url) key?)
    clj-deserializer))

;; Public API

(def ^{:const true
       :doc   "Provides handlers for all of Avro's fundamental types besides `fixed`.

  Fixed is unsupported."}

  +base-schema-type-registry+

  {;; Primitives
   {:type "boolean"} (fn [_ _] (BooleanType.))
   {:type "bytes"}   (fn [_ _] (BytesType.))
   {:type "double"}  (fn [_ _] (DoubleType.))
   {:type "float"}   (fn [_ _] (FloatType.))
   {:type "int"}     (fn [_ _] (IntType.))
   {:type "long"}    (fn [_ _] (LongType.))
   {:type "string"}  (fn [_ _] (StringType.))
   {:type "null"}    (fn [_ _] (NullType.))
   nil               (fn [_ _] (SchemalessType.))

   ;; Compounds
   {:type "array"}  ->ArrayType
   {:type "enum"}   ->EnumType
   {:type "map"}    ->MapType
   {:type "record"} ->RecordType
   {:type "union"}  ->UnionType

   ;; Unsupported
   {:type "fixed"} (fn [_ _] (throw (ex-info "The fixed type is unsupported" {})))})

(def ^{:const true
       :doc "A type constructor registry.

   Provides the logical types `uuid` and `jackdaw.serdes.avro.UUID` coded as strings with coercion
   to round-trip `java.util.UUID` instances."}

  +UUID-type-registry+

  { ;; Our "on by default" logicaltypes
   {:type         "string"
    :logical-type "jackdaw.serdes.avro.UUID"}
   (fn [_ _] (StringUUIDType.))

   {:type "string" :logical-type "uuid"}
   (fn [_ _] (StringUUIDType.))})

(defn serde
  "Given a type and logical type registry, a schema registry config with
  either a client or a URL and an Avro topic descriptor, build and
  return a Serde instance."
  [type-registry
   {:keys [avro.schema-registry/client
           avro.schema-registry/url]
    :as   registry-config}
   {:keys [avro/schema
           avro/coercion-cache
           key?]
    :as   topic-config}]

  (when-not url
    (throw
     (IllegalArgumentException.
      ":avro.schema-registry/url is required in the registry config")))

  (when-not (or (instance? clojure.lang.Atom coercion-cache)
                (nil? coercion-cache))
    (throw
     (IllegalArgumentException.
      ":avro/coercion-cache in the schema config must be either absent/nil, or an atom containing a cache")))

  (let [config {:key?            key?
                :registry-url    url
                :registry-client (or client
                                     (registry/client url 128))
                ;; Provide the old behavior by default, or fall through to the
                ;; new behavior of getting the right schema when possible.
                :avro-schema     (parse-schema-str schema)}

        ;; Coercion stack caching
        ;;
        ;; Every record carries its schema instance attached. Schemas implement reasonable hashing
        ;; and object equality. This means that, rather than building up the entire clj <-> avro
        ;; projection machinery by walking the schema for every record in or out, we can cache
        coercion-cache (or coercion-cache (atom (cache/lru-cache-factory {})))
        schema->coercion* (make-coercion-stack type-registry)
        ;; Note that while schema->coercion* is directly recursive, schema->coercion simply
        ;; delegates. Consequently there will be no cache entries other than encountered top level
        ;; Avro schemas.
        schema->coercion  #(locking coercion-cache
                             ;; This hits or fills the cache as a side-effect hence the locking
                             (swap! coercion-cache cache/through-cache % schema->coercion*)
                             ;; Read and return the value. In locking so we have RAW.
                             (get @coercion-cache %))

        ;; The final serdes based on the (cached) coercion stack.
        avro-serializer (serializer schema->coercion config)
        avro-deserializer (deserializer schema->coercion config)]
    (Serdes/serdeFrom avro-serializer avro-deserializer)))
