(ns kafka.streams.mock
  "Mocks for testing kafka streams."
  (:require [kafka.streams :as k]
            [kafka.streams.configurable :refer [config configure]]
            [kafka.streams.configured :as configured])
  (:import [org.apache.kafka.test KStreamTestDriver MockProcessorSupplier]))

(defn topology-builder
  "Creates a mock topology-builder."
  [topology-builder]
  (let [config {::topology-builder (k/topology-builder* topology-builder)
                ::processor-supplier (MockProcessorSupplier.)}]
    (configured/topology-builder config topology-builder)))

(defn build
  "Builds the topology."
  [topology]
  (.process (k/kstream* topology)
            (-> topology config ::processor-supplier)
            (into-array String []))
  (let [test-driver (KStreamTestDriver. (-> topology config ::topology-builder))]
    (-> topology
        (configure ::test-driver test-driver))))

(defn send!
  "Publishes message to a topic."
  [topology topic-config key message]
  (.process (-> topology config ::test-driver)
            (:topic.metadata/name topic-config)
            key
            message))

(defn collect
  "Collects the test results. The test driver returns a list of messages with
  each message formatted like \"key:value\""
  [topology-builder]
  (.processed (-> topology-builder config ::processor-supplier)))

(defn topic
  "Helper to create a topic."
  [topic-name]
  {:topic.metadata/name topic-name})
