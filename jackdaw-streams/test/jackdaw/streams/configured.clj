(ns jackdaw.streams.configured
  "Clojure wrapper to kafka streams."
  (:refer-clojure :exclude [count map reduce group-by merge filter])
  (:require [jackdaw.streams.protocols :refer :all]
            [jackdaw.streams.configurable :refer [config IConfigurable]]))

(declare configured-kstream configured-ktable configured-global-ktable configured-kgroupedtable configured-kgroupedstream)

(deftype ConfiguredTopologyBuilder [config topology-builder]
  ITopologyBuilder
  (merge
    [_ kstreams]
    (configured-kstream
     config
     (merge topology-builder kstreams)))

  (kstream
    [_ topic-config]
    (configured-kstream
     config
     (kstream topology-builder topic-config)))

  (kstream
    [_ topic-config topic-pattern]
    (configured-kstream
     config
     (kstream topology-builder topic-config topic-pattern)))

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

  (ktable
    [_ topic-config store-name]
    (configured-ktable
     config
     (ktable topology-builder topic-config store-name)))

  (global-ktable
    [_ topic-config]
    (configured-global-ktable
      config
      (global-ktable topology-builder topic-config)))

  (global-ktable
    [_ topic-config store-name]
    (configured-global-ktable
      config
      (global-ktable topology-builder topic-config store-name)))

  (source-topics
    [_]
    (source-topics topology-builder))

  (topology-builder*
    [_]
    (topology-builder* topology-builder))

  IConfigurable
  (config [_]
    config)

  (configure [_ key value]
    (ConfiguredTopologyBuilder.
     (assoc config key value)
     topology-builder)))

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

  (left-join
  [_ ktable value-joiner-fn topic-config]
  (configured-kstream
    config
    (left-join kstream ktable value-joiner-fn topic-config)))

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

  (group-by
    [_ key-value-mapper-fn]
    (configured-kgroupedstream
     config
     (group-by kstream key-value-mapper-fn)))

  (group-by
    [_ key-value-mapper-fn topic-config]
    (configured-kgroupedstream
     config
     (group-by kstream key-value-mapper-fn topic-config)))

  (map-values
    [_ value-mapper-fn]
    (configured-kstream
     config
     (map-values kstream value-mapper-fn)))

  (print!
    [_]
    (print! kstream))

  (print!
    [_ topic-config]
    (print! kstream topic-config))

  (through
    [_ topic-config]
    (configured-kstream
     config
     (through kstream topic-config)))

  (through
    [_ partition-fn topic-config]
    (configured-kstream
     config
     (through kstream partition-fn topic-config)))

  (to!
    [_ topic-config]
    (to! kstream topic-config))

  (to!
    [_ partition-fn topic-config]
    (to! kstream partition-fn topic-config))

  (write-as-text!
    [_ file-path]
    (write-as-text! kstream file-path))

  (write-as-text!
    [_ file-path topic-config]
    (write-as-text! kstream file-path topic-config))

  IKStream
  (branch
    [_ predicate-fns]
     (mapv (partial configured-kstream config)
           (branch kstream predicate-fns)))

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

  (group-by-key
    [_]
    (configured-kgroupedstream
     config
     (group-by-key kstream)))

  (group-by-key
    [_ topic-config]
    (configured-kgroupedstream
     config
     (group-by-key kstream topic-config)))

  (join-windowed
    [_ other-kstream value-joiner-fn windows]
    (configured-kstream
     config
     (join-windowed kstream
                    other-kstream
                    value-joiner-fn
                    windows)))

  (join-windowed
    [_ other-kstream value-joiner-fn windows topic-config other-topic-config]
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
    [_ other-kstream value-joiner-fn windows topic-config other-topic-config]
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
    [_ other-kstream value-joiner-fn windows topic-config other-topic-config]
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

  (select-key
    [_ key-value-mapper-fn]
    (configured-kstream
     config
     (select-key kstream key-value-mapper-fn)))

  (transform
      [this transformer-supplier-fn]
    (transform this transformer-supplier-fn []))

  (transform
    [_ transformer-supplier-fn state-store-names]
    (configured-kstream
     config
     (transform kstream transformer-supplier-fn state-store-names)))

  (transform-values
      [this value-transformer-supplier-fn]
    (transform-values this value-transformer-supplier-fn []))

  (transform-values
    [_ value-transformer-supplier-fn state-store-names]
    (configured-kstream
     config
     (transform-values kstream value-transformer-supplier-fn state-store-names)))

  (left-join-global
    [_ global-ktable kv-mapper joiner]
    (configured-kstream
      config
      (left-join-global kstream global-ktable kv-mapper joiner)))

  (join-global
    [_ global-ktable kv-mapper joiner]
    (configured-kstream
      config
      (join-global kstream global-ktable kv-mapper joiner)))

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
    [_ topic-config]
    (print! ktable topic-config))

  (through
    [_ topic-config]
    (configured-ktable
     config
     (through ktable topic-config)))

  (through
    [_ partition-fn topic-config]
    (configured-ktable
     config
     (through ktable partition-fn topic-config)))

  (to!
    [_ topic-config]
    (to! ktable topic-config))

  (to!
    [_ partition-fn topic-config]
    (to! ktable partition-fn topic-config))

  (write-as-text!
    [_ file-path]
    (write-as-text! ktable file-path))

  (write-as-text!
    [_ file-path topic-config]
    (write-as-text! ktable file-path topic-config))

  IKTable
  (group-by
    [_ key-value-mapper-fn]
    (configured-kgroupedtable
     config
     (group-by ktable key-value-mapper-fn)))

  (group-by
    [_ key-value-mapper-fn topic-config]
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

  (to-kstream
    [_]
    (configured-kstream
     config
     (to-kstream ktable)))

  (to-kstream
    [_ key-value-mapper-fn]
    (configured-kstream
     config
     (to-kstream ktable key-value-mapper-fn)))

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
  IKGroupedBase
  (aggregate
    [_ initializer-fn adder-fn subtractor-fn topic-config]
    (configured-ktable
     config
     (aggregate kgroupedtable initializer-fn adder-fn subtractor-fn topic-config)))

  (count
    [_ name]
    (configured-ktable
     config
     (count kgroupedtable name)))

  (reduce
    [_ adder-fn subtractor-fn topic-config]
    (configured-ktable
     config
     (reduce kgroupedtable adder-fn subtractor-fn topic-config)))

  IKGroupedTable
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

(deftype ConfiguredKGroupedStream [config kgroupedstream]
  IKGroupedBase
  (aggregate
    [_ initializer-fn aggregator-fn topic-config]
    (configured-ktable
     config
     (aggregate kgroupedstream initializer-fn aggregator-fn topic-config)))

  (count
    [_ name]
    (configured-ktable
     config
     (count kgroupedstream name)))

  (reduce
    [_ reducer-fn topic-config]
    (configured-ktable
     config
     (reduce kgroupedstream reducer-fn topic-config)))

  IKGroupedStream
  (aggregate-windowed
    [_ initializer-fn aggregator-fn windows topic-config]
    (configured-ktable
     config
     (aggregate-windowed kgroupedstream initializer-fn aggregator-fn windows topic-config)))

  (count-windowed
    [_ windows topic-config]
    (configured-ktable
     config
     (count-windowed kgroupedstream windows topic-config)))

  (reduce-windowed
    [_ reducer-fn windows topic-config]
    (configured-ktable
     config
     (reduce-windowed kgroupedstream reducer-fn windows topic-config)))

  (kgroupedstream*
    [_]
    kgroupedstream))


(defn configured-kgroupedstream
  "Makes a ConfiguredKGroupedStream object."
  [config kgroupedstream]
  (ConfiguredKGroupedStream. config kgroupedstream))

(deftype ConfiguredGlobalKTable [config global-ktable]
  IGlobalKTable
  (global-ktable*
    [_]
    (global-ktable* global-ktable))

  IConfigurable
  (config [_]
    config)

  (configure [_ key value]
    (configured-global-ktable
     (assoc config key value)
     global-ktable)))

(defn configured-global-ktable
  "Makes a ConfiguredKTable object."
  [config global-ktable]
  (ConfiguredGlobalKTable. config global-ktable))
