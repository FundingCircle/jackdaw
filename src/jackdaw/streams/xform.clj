(ns jackdaw.streams.xform
  "Helper functions for working with transducers."
  (:gen-class)
  (:require [jackdaw.serdes :as js]
            [jackdaw.streams :as j])
  (:import org.apache.kafka.streams.kstream.Transformer
           org.apache.kafka.streams.KeyValue
           [org.apache.kafka.streams.state KeyValueStore Stores]
           org.apache.kafka.streams.StreamsBuilder))

(defn kv-store-swap-fn
  "Takes an instance of KeyValueStore, a function f, and map m, and
  updates the store in a manner similar to `clojure.core/swap!`."
  [^KeyValueStore store f m]
  (let [ks (keys (f {} m))
        prev (reduce (fn [p k]
                       (assoc p k (.get store k)))
                     {}
                     ks)
        next (f prev m)]
    (doall (map (fn [[k v]] (.put store k v)) next))
    next))

(defn add-state-store!
  [builder]
  "Takes a builder and adds a state store."
  (doto ^StreamsBuilder (j/streams-builder* builder)
    (.addStateStore (Stores/keyValueStoreBuilder
                     (Stores/persistentKeyValueStore "transducer")
                     (js/edn-serde)
                     (js/edn-serde))))
  builder)

(defn transformer
  "Takes a transducer and creates an instance of
  org.apache.kafka.streams.kstream.Transformer with overrides for
  init, transform, and close."
  [xf]
  (let [ctx (atom nil)]
    (reify
      Transformer
      (init [_ context]
        (reset! ctx context))
      (transform [_ k v]
        (let [^KeyValueStore store (.getStateStore @ctx "transducer")
              v (first (into [] (xf store) [[k v]]))]
          (KeyValue/pair k v)))
      (close [_]))))

(defn transduce-kstream
  [kstream xf]
  "Takes a kstream and xf and transduces the stream."
  (-> kstream
      (j/transform (fn [] (transformer xf)) ["transducer"])
      (j/flat-map (fn [[_ v]] v))))
