(ns jackdaw.streams.mock
  "Mocks for testing kafka streams."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:refer-clojure :exclude [send])
  (:require [jackdaw.streams :as js]
            [jackdaw.streams.interop :as interop])
  (:import [org.apache.kafka.streams Topology TopologyTestDriver]
           java.util.Properties
           [org.apache.kafka.common.serialization Serde Serdes]
           [java.util NoSuchElementException]))

(set! *warn-on-reflection* false)

(defn topology->test-driver
  "Given a kafka streams topology, return a topology test driver for it"
  [topology]
  (TopologyTestDriver.
    topology
    (doto (Properties.)
      (.put "application.id"      (str (java.util.UUID/randomUUID)))
      (.put "bootstrap.servers"   "fake")
      (.put "default.key.serde"   "jackdaw.serdes.EdnSerde")
      (.put "default.value.serde" "jackdaw.serdes.EdnSerde"))))

(defn streams-builder->test-driver
  "Given the jackdaw streams builder, return a builds the described topology
  and returns a topology test driver for that topology"
  [streams-builder]
  (let [topology (.build (js/streams-builder* streams-builder))]
    (topology->test-driver topology)))

(defn producer
  "Returns a function which can be used to pubish data to a topic for the
  topology test driver"
  [^TopologyTestDriver test-driver
   {:keys [topic-name
           ^Serde key-serde
           ^Serde value-serde]}]
  (let [input-topic (.createInputTopic test-driver
                                       topic-name
                                       (.serializer key-serde)
                                       (.serializer value-serde))]
    (fn produce!
      ([k v]
       (.pipeInput input-topic k v))
      ([^long time-ms k v]
       (.pipeInput input-topic k v time-ms)))))

(defn publish
  ([test-driver topic-config k v]
   ((producer test-driver topic-config) k v))
  ([test-driver topic-config time-ms k v]
   ((producer test-driver topic-config) time-ms k v)))

(defn consume
  [^TopologyTestDriver test-driver
   {:keys [topic-name
           ^Serde key-serde
           ^Serde value-serde]}]
  (let [output-topic (.createOutputTopic test-driver
                                   topic-name
                                   (.deserializer key-serde)
                                   (.deserializer value-serde))
        record (try (.readKeyValue output-topic) (catch NoSuchElementException e))]
    (when record
      {:key (.-key record) :value (.-value record)})))

(defn repeatedly-consume
  [test-driver topic-config]
  (take-while some? (repeatedly (partial consume test-driver topic-config))))

(defn get-keyvals
  [test-driver topic-config]
  (map #((juxt :key :value) %) (repeatedly-consume test-driver topic-config)))

(defn get-records
  [test-driver topic-config]
  (repeatedly-consume test-driver topic-config))

(defn build-driver [f]
  (let [builder (interop/streams-builder)]
    (f builder)
    (streams-builder->test-driver builder)))

(defn build-topology-driver [f]
  (let [topology (Topology.)]
    (f topology)
    (topology->test-driver topology)))


;; FIXME (arrdem 2018-11-24):
;;   This is used by the test suite but has no bearing on anything else
(defn topic
  "Helper to create a topic."
  [topic-name]
  {:topic-name topic-name
   :key-serde (Serdes/Long)
   :value-serde (Serdes/Long)})
