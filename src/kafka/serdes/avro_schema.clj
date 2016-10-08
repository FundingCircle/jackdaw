(ns kafka.serdes.avro-schema
  (:import
   (org.apache.avro Schema
                    Schema$Field
                    Schema$Parser
                    Schema$Type)
   (org.apache.avro.generic GenericData$Array
                            GenericData$EnumSymbol
                            GenericData$Record
                            GenericEnumSymbol
                            GenericRecord)
   (org.apache.avro.util Utf8)))

;; from https://github.com/FundingCircle/avro-schemas/blob/3c2eec6098d8c51ffb1d09e02e733ccf55b5f1f9/src/avro_schemas/confluent.clj

(declare map->generic-record generic-record->map map->generic-union)

(defn- mangle [^String n]
  (clojure.string/replace n #"-" "_"))

(defn- unmangle [^String n]
  (clojure.string/replace n #"_" "-"))

;; GenericRecord to Clojure mapping

(defprotocol Unmarshal
  (value-unmarshal [in] "Converts from GenericData to Clojure value"))

(extend-protocol Unmarshal
  java.util.Map
  (value-unmarshal [val]
    (reduce (fn [m k]
              (assoc m
                     (keyword (value-unmarshal k))
                     (value-unmarshal (get val k))))
            {}
            (keys val)))

  Utf8
  (value-unmarshal [val]
    (.toString val))

  GenericEnumSymbol
  (value-unmarshal [val]
    (keyword (.toString val)))

  GenericData$Array
  (value-unmarshal [val]
    (mapv #(value-unmarshal %) (.toArray val)))

  GenericData$Record
  (value-unmarshal [val]
    (generic-record->map val))

  java.lang.Object
  (value-unmarshal [val]
    val)

  nil
  (value-unmarshal [val]
    val))

(defn generic-record->map [rec]
  (if (= (type rec) GenericData$Record)
    (reduce (fn [m field]
              (assoc m (keyword (unmangle (.name ^Schema$Field field)))
                     (value-unmarshal (.get rec (.name ^Schema$Field field)))))
            {}
            (.getFields ^Schema (.getSchema rec)))
    rec))

;; Clojure to GenericRecord mapping

(defn get-schema-type [^Schema s]
  (-> s
      .schema
      .getType))

(defn fields-for-schema [^Schema s & {:keys [value-function]
                              :or {value-function get-schema-type}}]
  (let [fields (.getFields s)
        names (map #(keyword (unmangle (.name %))) fields)
        types (map value-function fields)]
    (zipmap names types)))

(defn schema-by-field-name [^Schema$Field f ^Schema s]
  (let [get-schema #(.schema %)
        fields (fields-for-schema s :value-function get-schema)]
        (f fields)))

(defn map->generic-array [s m]
  (let [schema s
        element-schema (.getElementType s)
        element-schema-type (.getType element-schema)
        array (GenericData$Array. (count m) schema)]
    (doseq [value m]
      (let [return-value (condp = element-schema-type
                           Schema$Type/UNION (map->generic-union element-schema value)
                           Schema$Type/RECORD (map->generic-record element-schema value)
                           value)]
        (doto array (.add return-value))))
    array))


(defn match-schema? [s m]
  (let [schema-fields ((comp set keys fields-for-schema) s)
        map-fields ((comp set keys) m)]
    (= map-fields schema-fields)))

(defn map->generic-enum
  [field-schema v]
  (GenericData$EnumSymbol. field-schema (name v)))

(defn map->generic-union [s m]
  (loop [inner-types (.iterator (.getTypes s))
         result nil]
    (if result
      result
      (let [current-type (.next inner-types)]
          (condp = (.getType current-type)
            Schema$Type/NULL (if m (recur inner-types nil) nil)
            Schema$Type/ARRAY (recur nil (map->generic-array current-type m))
            Schema$Type/ENUM (map->generic-enum current-type m)
            Schema$Type/STRING (if (string? m) m (recur inner-types nil))
            Schema$Type/RECORD (if (and (map? m)
                                        (match-schema? current-type m))
                                 (map->generic-record current-type m)
                                 (recur inner-types nil))
            m)))))

(defn- populate-generic-record [^Schema schema ^GenericData$Record record [k v]]
  (let [fields (fields-for-schema schema)
        mangled-key (-> k name mangle)
        field-schema (schema-by-field-name k schema)]
    (condp = (k fields)
      Schema$Type/RECORD (doto record (.put mangled-key (map->generic-record field-schema v)))
      Schema$Type/ARRAY (doto record (.put mangled-key (map->generic-array field-schema v)))
      Schema$Type/UNION (doto record (.put mangled-key (map->generic-union field-schema v)))
      Schema$Type/ENUM (doto record (.put mangled-key (map->generic-enum field-schema v)))
      (doto record (.put mangled-key v)))))

(defn parse-schema
  "Parse a JSON schema string into a Schema object"
  [schema]
  (.parse (Schema$Parser.) schema))

(defn map->generic-record [schema m]
  (let [schema (parse-schema schema)
        record (GenericData$Record. schema)]
    (reduce (partial populate-generic-record schema) record m)))
