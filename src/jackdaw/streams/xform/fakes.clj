(ns jackdaw.streams.xform.fakes
  (:import
   [org.apache.kafka.streams.state KeyValueStore]))

(defn fake-kv-store
  "Creates an instance of org.apache.kafka.streams.state.KeyValueStore
  with overrides for get and put."
  [init]
  (let [store (volatile! init)]
    (reify KeyValueStore
      (get [_ k]
        (clojure.core/get @store k))

      (put [_ k v]
        (vswap! store assoc k v)))))
