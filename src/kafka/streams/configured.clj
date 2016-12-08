(ns kafka.streams.configured
  "Clojure wrapper to kafka streams."
  (:refer-clojure :exclude [count map reduce group-by merge filter])
  (:require [kafka.streams :refer :all]
            [kafka.streams.configurable :refer [config IConfigurable]]))

(declare configured-kstream configured-ktable configured-kgroupedtable)

(deftype ConfiguredTopologyBuilder [config topology-builder]
  ITopologyBuilder
  (merge
    [_ kstream]
    (mapv (partial configured-kstream config)
          (merge topology-builder kstream)))

  (new-name
    [_ prefix]
    (new-name topology-builder prefix))

  (kstream
    [_ topic-config]
    (configured-kstream
     config
     (kstream topology-builder topic-config)))

  (kstreams
    [_ topic-configs]
    (configured-kstream
     config
     (kstreams topology-builder topic-configs)))

  (ktable
    [_ topic-config]
    (configured-ktable
     config
     (ktable topology-builder topic-config)))

  (topology-builder*
    [_]
    (topology-builder* topology-builder))

  IConfigurable
  (config [_]
    config))

(defn topology-builder
  "Makes a topology builder."
  ([config topology-builder]
   (ConfiguredTopologyBuilder. config topology-builder)))

(deftype ConfiguredKStream [config kstream]
  IKStreamBase
  (left-join
    [_ ktable value-joiner-fn]
    (configured-kstream
     config
     (left-join kstream ktable value-joiner-fn)))

  (for-each!
    [_ foreach-fn]
    (for-each! kstream foreach-fn))

  (filter
    [_ predicate-fn]
    (configured-kstream
     config
     (filter kstream predicate-fn)))

  (filter-not
    [_ predicate-fn]
    (configured-kstream
     config
     (filter-not kstream predicate-fn)))

  (map-values
    [_ value-mapper-fn]
    (configured-kstream
     config
     (map-values kstream value-mapper-fn)))

  (print!
    [_]
    (print! kstream))

  (print!
    [_ {:keys [kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (print! kstream topic-config))

  (through
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-kstream
     config
     (through kstream topic-config)))

  (through
    [_ partition-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-kstream
     config
     (through kstream partition-fn topic-config)))

  (to!
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (to! kstream topic-config))

  (to!
    [_ partition-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (to! kstream partition-fn topic-config))

  (write-as-text!
    [_ file-path]
    (write-as-text! kstream file-path))

  (write-as-text!
    [_ file-path {:keys [kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (write-as-text! kstream file-path topic-config))

  IKStream
  (aggregate-by-key
    [_ initializer-fn aggregator-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-kstream
     config
     (aggregate-by-key kstream initializer-fn aggregator-fn topic-config)))

  (aggregate-by-key-windowed
    [_ initializer-fn aggregator-fn windows]
    (configured-kstream
     config
     (aggregate-by-key-windowed kstream initializer-fn aggregator-fn windows)))

  (aggregate-by-key-windowed
    [_ initializer-fn aggregator-fn windows  {:keys [kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-kstream
     config
     (aggregate-by-key-windowed kstream initializer-fn aggregator-fn windows topic-config)))

  (branch
    [_ predicate-fns]
     (mapv (partial configured-kstream config)
           (branch kstream predicate-fns)))

  (count-by-key
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde] :as topic-config}]
    (configured-ktable
     config
     (count-by-key kstream topic-config)))

  (count-by-key-windowed
    [_ windows]
    (configured-ktable
     config
     (count-by-key-windowed kstream windows)))

  (count-by-key-windowed
    [_ windows {:keys [kafka.serdes/key-serde] :as topic-config}]
    (configured-ktable
     config
     (count-by-key-windowed kstream topic-config)))

  (flat-map
    [_ key-value-mapper-fn]
    (configured-kstream
     config
     (flat-map kstream key-value-mapper-fn)))

  (flat-map-values
    [_ value-mapper-fn]
    (configured-kstream
     config
     (flat-map-values kstream value-mapper-fn)))

  (join-windowed
    [_ other-kstream value-joiner-fn windows]
    (configured-kstream
     config
     (join-windowed kstream
                    other-kstream
                    value-joiner-fn
                    windows)))

  (join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :kafka.serdes/key-serde this-value-serde :kafka.serdes/value-serde :as topic-config}
     {other-value-serde :kafka.serdes/value-serde :as other-topic-config}]
    (configured-kstream
     config
     (join-windowed kstream
                    other-kstream
                    value-joiner-fn
                    windows
                    topic-config
                    other-topic-config)))

  (left-join-windowed
    [_ other-kstream value-joiner-fn windows]
    (configured-kstream
     config
     (left-join-windowed kstream other-kstream value-joiner-fn windows)))

  (left-join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :kafka.serdes/key-serde :as topic-config}
     {other-value-serde :kafka.serdes/value-serde :as other-topic-config}]
    (configured-kstream
     config
     (left-join-windowed kstream
                         other-kstream
                         value-joiner-fn
                         windows
                         topic-config
                         other-topic-config)))

  (map
    [_ key-value-mapper-fn]
    (configured-kstream
     config
     (map kstream key-value-mapper-fn)))

  (outer-join-windowed
    [_ other-kstream value-joiner-fn windows]
    (configured-kstream
     config
     (outer-join-windowed kstream
                          other-kstream
                          value-joiner-fn
                          windows)))

  (outer-join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :kafka.serdes/key-serde value-serde :kafka.serdes/value-serde :as topic-config}
     {other-value-serde :kafka.serdes/value-serde :as other-topic-config}]
    (configured-kstream
     config
     (outer-join-windowed kstream
                          other-kstream
                          value-joiner-fn
                          windows
                          topic-config
                          other-topic-config)))

  (process!
    [_ processor-supplier-fn state-store-names]
    (process! kstream processor-supplier-fn state-store-names))

  (reduce-by-key
   [_ reducer-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-ktable
     config
     (reduce-by-key kstream reducer-fn topic-config)))

  (reduce-by-key-windowed
    [_ reducer-fn windows]
    (configured-ktable
     config
     (reduce-by-key-windowed kstream reducer-fn windows)))

  (reduce-by-key-windowed
   [_ reducer-fn windows {:keys [kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-ktable
     config
     (reduce-by-key-windowed kstream reducer-fn windows topic-config)))

  (select-key
    [_ key-value-mapper-fn]
    (configured-kstream
     config
     (select-key kstream key-value-mapper-fn)))

  (transform
    [_ transformer-supplier-fn state-store-names]
    (configured-kstream
     config
     (transform kstream transformer-supplier-fn state-store-names)))

  (transform-values
    [_ transformer-supplier-fn state-store-names]
    (configured-kstream
     config
     (transform-values kstream transformer-supplier-fn state-store-names)))

  (kstream* [_]
    (kstream* kstream))

  IConfigurable
  (config [_]
    config)

  (configure [_ key value]
    (configured-kstream
     (assoc config key value)
     kstream)))

(defn configured-kstream
  "Makes a ConfiguredStream object."
  [config kstream]
  (ConfiguredKStream. config kstream))

(deftype ConfiguredKTable [config ktable]
  IKStreamBase
  (left-join
    [_ other-ktable value-joiner-fn]
    (configured-ktable
     config
     (left-join ktable other-ktable value-joiner-fn)))

  (for-each!
    [_ foreach-fn]
    (for-each! ktable foreach-fn))

  (filter
    [_ predicate-fn]
    (configured-ktable
     config
     (filter ktable predicate-fn)))

  (filter-not
    [_ predicate-fn]
    (configured-ktable
     config
     (filter-not ktable predicate-fn)))

  (map-values
    [_ value-mapper-fn]
    (configured-ktable
     config
     (map-values ktable value-mapper-fn)))

  (print!
    [_]
    (print! ktable))

  (print!
    [_ {:keys [kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (print! ktable topic-config))

  (through
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-ktable
     config
     (through ktable topic-config)))

  (through
    [_ partition-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-ktable
     config
     (through ktable partition-fn topic-config)))

  (to!
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (to! ktable topic-config))

  (to!
    [_ partition-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (to! ktable partition-fn topic-config))

  (write-as-text!
    [_ file-path]
    (write-as-text! ktable file-path))

  (write-as-text!
    [_ file-path {:keys [kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (write-as-text! ktable file-path topic-config))

  IKTable
  (group-by
    [_ key-value-mapper-fn]
    (configured-kgroupedtable
     config
     (group-by ktable key-value-mapper-fn)))

  (group-by
    [_ key-value-mapper-fn {:keys [kafka.serdes/key-serde kafka.serdes/value-serde] :as topic-config}]
    (configured-kgroupedtable
     config
     (group-by ktable key-value-mapper-fn topic-config)))

  (join
    [_ other-ktable value-joiner-fn]
    (configured-ktable
     config
     (join ktable other-ktable value-joiner-fn)))

  (outer-join
    [_ other-ktable value-joiner-fn]
    (configured-ktable
     config
     (outer-join ktable other-ktable value-joiner-fn)))

  (ktable* [_]
    (ktable* ktable))

  IConfigurable
  (config [_]
    config)

  (configure [_ key value]
    (configured-ktable
     (assoc config key value)
     ktable)))

(defn configured-ktable
  "Makes a ConfiguredKTable object."
  [config ktable]
  (ConfiguredKTable. config ktable))

(deftype ConfiguredKGroupedTable [config kgroupedtable]
  IKGroupedTable
  (aggregate
    [_ initializer-fn adder-fn subtractor-fn
     {:keys [topic.metadata/name kafka.serdes/value-serde] :as topic-config}]
    (configured-ktable
     config
     (aggregate kgroupedtable initializer-fn adder-fn subtractor-fn topic-config)))

  (count
    [_]
    (configured-ktable
     config
     (count kgroupedtable)))

  (reduce
    [_ adder-fn subtractor-fn {:keys [topic.metadata/name] :as topic-config}]
    (configured-ktable
     config
     (reduce kgroupedtable adder-fn subtractor-fn topic-config)))

  (kgroupedtable*
    [_]
    (kgroupedtable* kgroupedtable))

  IConfigurable
  (config [_]
    config)

  (configure [_ key value]
    (configured-kgroupedtable
     (assoc config key value)
     kgroupedtable)))

(defn configured-kgroupedtable
  "Makes a ConfiguredKGroupedTable object."
  [config kgroupedtable]
  (ConfiguredKGroupedTable. config kgroupedtable))
