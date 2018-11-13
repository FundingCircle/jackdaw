(ns jackdaw.serdes
  "Some useful serdes."
  (:refer-clojure :exclude [resolve])
  (:require [jackdaw.serdes.edn.nippy :as j.s.edn.nippy]
            [jackdaw.serdes.json :as json]
            [jackdaw.serdes.uuid :as uuid])
  (:import org.apache.kafka.common.serialization.Serdes))

(def
  ^{:const true
    :doc "A registry of keys to functions of a topic config to a SerDe instance.

  This registry is deliberately somewhat incomplete, as users are intended to
  call `#'resolve` or `#'serde` as appropriate with their own mappings.

  This mapping is not intended to be exhaustive, merely introductory and useful."}
  +default-serdes+

  {::edn         (fn [_] (j.s.edn.nippy/serde))
   ::json        (fn [_] (json/json-serde))
   ::uuid        (fn [_] (uuid/uuid-serde))
   ::byte-array  (fn [_] (Serdes/ByteArray))
   ::byte-buffer (fn [_] (Serdes/ByteBuffer))
   ::double      (fn [_] (Serdes/Double))
   ::integer     (fn [_] (Serdes/Integer))
   ::long        (fn [_] (Serdes/Long))
   ::string      (fn [_] (Serdes/String))})

(defn serde
  "Accepts an optional serde ctor `mapping`, a topic configuration
  specifying a serde or a keyword naming a serde, and an optional
  `default-ctor` serde constructor.

  Looks a serde constructor up out of the mapping and applies the
  constructor to the given topic configuration (or keyword), returning
  its result - presumably a serde.

  If no mapping is provided, `#'+default-serdes+` is used.

  If no serde constructor is found in the active mapping, the
  `default-ctor` is invoked.

  If no `default-ctor` is provided, a function which simply throws
  `ex-info` indicating a failure to resolve a serde is used."

  ([topic-or-kw]
   (serde +default-serdes+
          topic-or-kw))

  ([mapping topic-or-kw]
   (serde mapping
          topic-or-kw
          (fn [t]
            (throw (ex-info "Unable to resolve serde for topic"
                            {:topic topic-or-kw})))))

  ([mapping topic-or-kw default-ctor]
   (let [key (or (::type topic-or-kw)
                 topic-or-kw)]
     (when-not (keyword? key)
       (throw (ex-info "Failed to find a legal mapping `key` for `topic-or-kw`"
                       {:topic-or-kw topic-or-kw
                        :key key})))
     (-> (get mapping key default-ctor)
         ;; Could also be (apply <> topic-or-kw nil)
         (.invoke topic-or-kw)))))

(defn resolve
  "Loads the key and value serdes for a topic spec, returning an extended topic spec.

  By default, serdes are resolved from `#'+default-serdes+`. The
  registry is deliberately read-only. Users wanting to override the
  default registry serdes, or to otherwise bring their own serdes
  should simply use the 2-arity and provide a serde ctor registry
  suiting their needs.

  This is done so that no global coherence property is required on
  serde instances, as it's impossible to enforce such a contract.

  `topic-config` structs are expected to have
  `:jackdaw.topic/key-serde` and `:jackdaw.topic/value-serde`, both of
  which should be keywords present in the provided (or default) serde
  registry mappings.

  In order to support Avro serdes, which are parameterized on the
  concrete schema to use, `:jackdaw.topic/value-schema` and
  `:jackdaw.topics/key-schema` may be provided."

  ([topic-config]
   (resolve +default-serdes+ topic-config))
  ([registry topic-config]
   (let [{:keys [jackdaw.topic/key-serde
                 jackdaw.topic/value-serde]} topic-config]

     (if-not key-serde
       (throw
        (IllegalArgumentException.
         ":jackdaw.topic/key-serde is required in the topic config.")))

     (if-not value-serde
       (throw
        (IllegalArgumentException.
         ":jackdaw.topic/value-serde is required in the topic config.")))

     (assoc topic-config
            ;; FIXME (reid.mckenzie 2018-09-24):
            ;;
            ;;   Passing the desired serde through with `::type` and implicitly
            ;;   parameterizing the ctor on the thus altered topic config is
            ;;   fairly janky. There's probably a better way to generally
            ;;   parameterize serdes, but leaving this as is for now.
            ::key-serde
            (serde registry
                   (assoc topic-config
                          ::type key-serde))
            ::value-serde
            (serde registry
                   (assoc topic-config
                          ::type value-serde))))))
