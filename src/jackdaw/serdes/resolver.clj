(ns jackdaw.serdes.resolver
  "Helper function for creating serdes."
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [jackdaw.serdes.avro.confluent]
            [jackdaw.serdes.edn]
            [jackdaw.serdes.json]))

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
  :serde-qualified-keyword key."
  [{:keys [serde-qualified-keyword] :as serde-config}]
  (let [the-var (resolve (symbol (namespace serde-qualified-keyword)
                                 (name serde-qualified-keyword)))]
    (when (nil? the-var)
      (let [msg "Could not resolve :serde-qualified-keyword value to a serde function"]
        (throw (ex-info msg (select-keys serde-config [:serde-qualified-keyword])))))
    the-var))

(defn serde-resolver
  "Returns a function of arity one which takes a serde config map and
  replaces (resolves) it with the implementation for the serde. The
  config map consists of a :serde-qualified-keyword key, and
  optionally, keys to specify the schema or schema filename and
  whether the serde will be used for an Avro key. The options are
  extra arguments which may be needed to create the serde, e.g, the
  schema registry URL.

  Options:
  schema-registry-url - The URL for the schema registry
  type-registry - A mapping per jackdaw.serdes.avro/+base-schema-type-registry+>

  These are only needed for the Confluent Avro serde, and even then
  only the schema registry URL is required."

  [& options]
  (let [{:keys [type-registry schema-registry-url schema-registry-client]}
        (apply hash-map options)]

    (fn [{:keys [serde-qualified-keyword
                 schema
                 schema-filename
                 key?]
          :as serde-config}]

      (if (not (s/valid? :jackdaw.specs/serde-qualified-keyword
                         serde-qualified-keyword))
        (throw (ex-info "Invalid serde config."
                        (s/explain-data :jackdaw.specs/serde-qualified-keyword
                                        serde-qualified-keyword)))

        (as-> serde-config %

          (if (some? schema-filename)
            (assoc % :schema (load-schema %))
            %)

          (assoc % ::serde (find-serde-var %))

          (if (some? (:schema %))
            (if (not (s/valid? :jackdaw.serde/confluent-avro-serde %))
              (throw (ex-info "Invalid serde config."
                              (s/explain-data :jackdaw.serde/confluent-avro-serde %)))
              ((::serde %) schema-registry-url (:schema %) key?
               {:type-registry type-registry
                :schema-registry-client schema-registry-client}))
            ((::serde %))))))))
