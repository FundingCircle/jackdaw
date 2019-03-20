(ns jackdaw.streams.mock
  "Mocks for testing kafka streams."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:refer-clojure :exclude [send])
  (:require [jackdaw.streams.protocols :as k]
            [jackdaw.streams.configurable :refer [config configure]]
            [jackdaw.streams.configured :as configured]
            [jackdaw.streams.interop :as interop]
            [jackdaw.data :as data])
  (:import java.nio.file.Files
           java.nio.file.attribute.FileAttribute
           org.apache.kafka.streams.TopologyTestDriver
           java.util.Properties
           org.apache.kafka.streams.test.ConsumerRecordFactory
           org.apache.kafka.common.header.internals.RecordHeaders
           [org.apache.kafka.common.serialization Serde Serdes Serializer]))

(defn streams-builder
  "Creates a mock streams-builder."
  ([]
   (streams-builder (interop/streams-builder)))
  ([streams-builder]
   (configured/streams-builder
    {::streams-builder (k/streams-builder* streams-builder)}
    streams-builder)))

(defn streams-builder->test-driver
  ""
  [streams-builder]
  (let [topology (-> streams-builder config ::streams-builder .build)]
    (TopologyTestDriver.
     topology
     (doto (Properties.)
       (.put "application.id"      (str (java.util.UUID/randomUUID)))
       (.put "bootstrap.servers"   "fake")
       (.put "default.key.serde"   "jackdaw.serdes.EdnSerde")
       (.put "default.value.serde" "jackdaw.serdes.EdnSerde")))))

(defn send
  "Publishes message to a topic."
  [topology topic-config key message]
  (let [test-driver (-> topology config ::test-driver)
        time (or (-> topology config ::test-driver-time) 0)]
    (.setTime test-driver time)
    (.process test-driver
              (:topic-name topic-config)
              key
              message)
    (.flushState test-driver)
    (-> topology
        (configure ::test-driver-time (inc time)))))

(defn collect
  "Collects the test results. The test driver returns a list of messages with
  each message formatted like \"key:value\""
  [streams-builder]
  (let [processor-supplier (-> streams-builder config ::processor-supplier)
        processed (vec (.processed processor-supplier))]
    (.clear (.processed processor-supplier))
    processed))

(defn producer
  ""
  [test-driver
   {:keys [topic-name
           ^Serde key-serde
           ^Serde value-serde]}]
  (let [record-factory (ConsumerRecordFactory.
                        topic-name
                        (.serializer key-serde)
                        (.serializer value-serde))]
    (fn produce!
      ([k v]
       (.pipeInput test-driver (.create record-factory k v)))
      ([time-ms k v]
       (.pipeInput test-driver (.create record-factory topic-name k v (RecordHeaders.) time-ms))))))

(defn publish
  ([test-driver topic-config k v]
   ((producer test-driver topic-config) k v))
  ([test-driver topic-config time-ms k v]
   ((producer test-driver topic-config) time-ms k v)))

(defn consume
  [test-driver
   {:keys [topic-name
           ^Serde key-serde
           ^Serde value-serde]}]
  (let [record (.readOutput test-driver topic-name
                            (.deserializer key-serde)
                            (.deserializer value-serde))]
    (when record
      (data/datafy record))))

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
  (let [builder (streams-builder)]
    (f builder)
    (streams-builder->test-driver builder)))

;; FIXME (arrdem 2018-11-24):
;;   This is used by the test suite but has no bearing on anything else
(defn topic
  "Helper to create a topic."
  [topic-name]
  {:topic-name topic-name
   :key-serde (Serdes/Long)
   :value-serde (Serdes/Long)})
