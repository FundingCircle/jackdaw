(ns jackdaw.streams.mock
  "Mocks for testing kafka streams."
  (:refer-clojure :exclude [send])
  (:require [jackdaw.streams.protocols :as k]
            [jackdaw.streams.configurable :refer [config configure]]
            [jackdaw.streams.configured :as configured]
            [jackdaw.streams.interop :as interop])
  (:import [org.apache.kafka.test KStreamTestDriver MockProcessorSupplier]
           java.nio.file.Files
           java.nio.file.attribute.FileAttribute
           org.apache.kafka.streams.TopologyTestDriver
           java.util.Properties
           org.apache.kafka.streams.test.ConsumerRecordFactory
           [org.apache.kafka.common.serialization Serdes Serializer]))

(defn streams-builder
  "Creates a mock streams-builder."
  ([]
   (streams-builder (interop/streams-builder)))
  ([streams-builder]
   (configured/streams-builder
    {::streams-builder (k/streams-builder* streams-builder)}
    streams-builder)))

(defn build
  "Builds the topology."
  [topology]
  (let [processor-supplier (MockProcessorSupplier.)]
    (.process (k/kstream* topology)
              processor-supplier
              (into-array String []))
    (let [test-driver (doto (KStreamTestDriver.)
                        (.setUp (-> topology config ::streams-builder)
                                (.toFile
                                 (Files/createTempDirectory
                                  "kstream-test-driver"
                                  (into-array FileAttribute [])))))]
      (-> topology
          (configure ::test-driver test-driver)
          (configure ::processor-supplier processor-supplier)))))

(defn streams-builder->test-driver [streams-builder]
  (let [topology (-> streams-builder config :jackdaw.streams.mock/streams-builder .build)]
    (TopologyTestDriver. topology (doto (Properties.)
                                    (.put "bootstrap.servers" "fake")
                                    (.put "application.id" (str (java.util.UUID/randomUUID)))))))

(defn send
  "Publishes message to a topic."
  [topology topic-config key message]
  (let [test-driver (-> topology config ::test-driver)
        time (or (-> topology config ::test-driver-time) 0)]
    (.setTime test-driver time)
    (.process test-driver
              (:jackdaw.topic/topic-name topic-config)
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
        processed (into [] (.processed processor-supplier))]
    (.clear (.processed processor-supplier))
    processed))

(defn producer [test-driver {:keys [jackdaw.topic/topic-name jackdaw.serdes/key-serde jackdaw.serdes/value-serde]}]
  (let [record-factory (ConsumerRecordFactory. topic-name
                                               ^Serializer (.serializer key-serde)
                                               ^Serializer (.serializer value-serde))]
    (fn produce!
      ([[k v]]
       (.pipeInput test-driver
                   (.create record-factory k v)))
      ([[k v] time-ms]
       (.pipeInput test-driver
                   (.create record-factory k v time-ms))))))

(defn producer-record [x]
  [(.key x) (.value x)])

(defn consume [test-driver {:keys [jackdaw.topic/topic-name jackdaw.serdes/key-serde jackdaw.serdes/value-serde]}]
  (let [record (.readOutput test-driver topic-name
                            (.deserializer key-serde)
                            (.deserializer value-serde))]
    (when record
      (producer-record record))))

(defn build-driver [f]
  (let [builder (streams-builder)]
    (f builder)
    (streams-builder->test-driver builder)))

(defn topic
  "Helper to create a topic."
  [topic-name]
  {:jackdaw.topic/topic-name topic-name
   :jackdaw.serdes/key-serde (Serdes/Long)
   :jackdaw.serdes/value-serde (Serdes/Long)})
