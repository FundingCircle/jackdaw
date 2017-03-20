(ns kafka.serdes.avro-schema
  (:import [org.apache.avro Schema Schema$Parser Schema$Field Schema$Type]
           [org.apache.avro.generic GenericData GenericEnumSymbol GenericData$Record GenericRecord GenericData$Array GenericData$EnumSymbol]
           [org.apache.avro.util Utf8]
           [java.util UUID]))

;; auxiliary functions

(defn- mangle [^String n]
  (clojure.string/replace n #"-" "_"))

(defn- unmangle [^String n]
  (clojure.string/replace n #"_" "-"))

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

(defn uuid-schema?
  "Return true if a `Schema` object has a \"logicalType\" property of
  \"kafka.serdes.avro.UUID\"."
  [s]
  (= (.getProp s "logicalType") "kafka.serdes.avro.UUID"))

;; Parser

(defmulti accept? (fn [schema v] (.getType schema)))
(defmethod accept? Schema$Type/RECORD [_ v] (map? v))
(defmethod accept? Schema$Type/ARRAY  [_ v] (vector? v))
(defmethod accept? Schema$Type/STRING [s v] (or (string? v)
                                                (uuid-schema? s)))
(defmethod accept? Schema$Type/NULL [_ v] (nil? v))
(defmethod accept? :default [_ _] true)


(defmulti marshall (fn [schema v]
                     (if (accept? schema v)
                      (.getType schema)
                      (throw (Exception. (str "Value not accepted for parser " schema ": " v))))))

(defmethod marshall Schema$Type/RECORD [s v]
  (let [record (GenericData$Record. s)]

    (letfn [(schema-by-field-name [^Schema$Field f ^Schema s]
            (let [get-schema #(.schema %)
                  fields (fields-for-schema s :value-function get-schema)]
              (f fields)))

          (marshall-for-field [record [k v]]
            (let [fields (fields-for-schema s)
                  mangled-key (-> k name mangle)
                  field-schema (schema-by-field-name k s)]
              (doto record (.put mangled-key (marshall field-schema v)))))]

      (reduce marshall-for-field record v))))

(defmethod marshall Schema$Type/ENUM [s v]
  (GenericData$EnumSymbol. s (mangle (name v))))


(defmethod marshall Schema$Type/LONG [s v] (long v))

(defmethod marshall Schema$Type/ARRAY [s v]
  (let [array (GenericData$Array. (count v) s)
        element-schema-type (.getElementType s)]
    (doseq [e v]
      (doto array (.add (marshall element-schema-type e))))
    array))

(defmethod marshall Schema$Type/UNION [s v]
  (let [inner-types (iterator-seq (.iterator (.getTypes s)))
        found-value (->> inner-types
                         (map #(try
                                 (marshall % v)
                                 (catch Exception _ ::not-valid)))
                         (filter #(not= % ::not-valid)))]
    (if (empty? found-value)
      (throw (Exception. "Cannot find valid union schema for value " v))
      (first found-value))))

(defmethod marshall Schema$Type/STRING [s v]
  (if (uuid-schema? s) (str v) v))

(defmethod marshall :default [_ v] v)

;; GenericRecord to Clojure mapping

(declare generic-record->map)

(defprotocol Unmarshal
  (value-unmarshal [in] "Converts from GenericData to Clojure value"))

(extend-protocol Unmarshal
  java.util.Map
  (value-unmarshal [val]
    (reduce (fn [m k]
              (assoc m
                     (value-unmarshal k)
                     (value-unmarshal (get val k))))
            {}
            (keys val)))

  Utf8
  (value-unmarshal [val]
    (.toString val))

  GenericEnumSymbol
  (value-unmarshal [val]
    (keyword (unmangle (.toString val))))

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
              (assoc m
                     (keyword (unmangle (.name ^Schema$Field field)))
                     (let [v (value-unmarshal (.get rec (.name ^Schema$Field field)))]
                       (if (and (string? v)
                                (or (uuid-schema? field)
                                    (uuid-schema? (.schema field))))
                         (UUID/fromString ^String v)
                         v))))
            {}
            (.getFields ^Schema (.getSchema rec)))
    rec))

(def parse-schema
  "Parse a JSON schema string into a Schema object"
  (memoize (fn [schema]
             (.parse (Schema$Parser.) schema))))

(defn map->generic-record [schema m]
  (marshall (parse-schema schema) m))

;(defn map->generic-record [schema m]
;  (let [schema (if (string? schema) (parse-schema schema) schema)
;        record (GenericData$Record. schema)]
;    (reduce (partial populate-generic-record schema) record m)))
