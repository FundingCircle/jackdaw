(ns jackdaw.serdes.resolver
  "Helper function for creating serdes."
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [jackdaw.serdes.avro.confluent]
            [jackdaw.serdes.json-schema.confluent]
            [jackdaw.serdes.edn]
            [jackdaw.serdes.json]
            [jackdaw.serdes]
            [jackdaw.specs]))

(set! *warn-on-reflection* true)

(defn load-schema
  "Takes a serde config and loads the schema from the classpath."
  [{:keys [schema-filename] :as serde-config}]
  (if schema-filename
    (try
      (slurp (io/resource schema-filename))
      (catch Exception _
        (throw (ex-info (str "Could not find schema " schema-filename) serde-config))))
    (throw (ex-info "No :schema-filename defined in serde config" serde-config))))

(defn find-serde-var
  "Takes a serde config and returns the var for its
  :serde-keyword key."
  [{:keys [serde-keyword] :as serde-config}]
  (let [the-var (resolve (symbol (namespace serde-keyword) (name serde-keyword)))]
    (when (nil? the-var)
      (let [msg "Could not resolve :serde-keyword value to a serde function"]
        (throw (ex-info msg (select-keys serde-config [:serde-keyword])))))
    the-var))

(defn serde-resolver
  "Returns a function of arity one which takes a serde config map and
  replaces (resolves) it with the implementation for the serde. The
  config map consists of a :serde-keyword key, and optionally, keys to
  specify the schema or schema filename and whether the serde will be
  used for an Avro key. The options are extra arguments which may be
  needed to create the serde, e.g, the schema registry URL.

  Options:
  schema-registry-url - The URL for the schema registry
  type-registry - A mapping per jackdaw.serdes.avro/+base-schema-type-registry+>
  read-only - Specifies that you will not be using the resulting serializer,
              and does not require a schema or schema-filename
  serializer-properties - Properties to be used when creating the serializer
  deserializer-properties - Properties to be used when creating the deserializer

  These are only needed for the Confluent Avro serde, and even then
  only the schema registry URL is required."

  [& options]
  (let [{:keys [type-registry schema-registry-url schema-registry-client
                serializer-properties deserializer-properties]}
        (apply hash-map options)]

    (fn [{:keys [serde-keyword schema schema-filename key? read-only?] :as serde-config}]
      (when-not (s/valid? :jackdaw.specs/serde-keyword serde-keyword)
        (throw (ex-info "Invalid serde config."
                        (s/explain-data :jackdaw.specs/serde-keyword serde-keyword))))
      (let [schema (cond
                     schema-filename (load-schema serde-config)
                     schema          schema
                     :else           nil)
            serde-fn (find-serde-var serde-config)]
        (case serde-keyword
          :jackdaw.serdes.json-schema.confluent/serde
          (serde-fn schema-registry-url schema key? {:schema-registry-client schema-registry-client
                                                     :read-only? read-only?
                                                     :serializer-properties serializer-properties
                                                     :deserializer-properties deserializer-properties})
          :jackdaw.serdes.avro.confluent/serde
          (if-not (s/valid? :jackdaw.serde/confluent-avro-serde serde-config)
            (throw (ex-info "Invalid serde config."
                            (s/explain-data :jackdaw.serde/confluent-avro-serde serde-config)))
            (serde-fn schema-registry-url schema key? {:type-registry type-registry
                                                       :schema-registry-client schema-registry-client
                                                       :read-only? read-only?
                                                       :serializer-properties serializer-properties
                                                       :deserializer-properties deserializer-properties}))
          (serde-fn))))))
