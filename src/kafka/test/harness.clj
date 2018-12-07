(ns kafka.test.harness
  "Lifecycle component for an embedded Kafka cluster

   The components defined here use the component lifecycle to model
   the dependencies between the various services (zookeeper, kafka broker
   and kafka client).

   The test-harness function returns a system containing all the relevent
   components, and starting that system starts the components in the
   correct order (i.e. zookeeper, kafka, test-client).

   When adding a new component here, please ensure it's `start` method
   *blocks* until the component is ready to accept requests from other
   components"
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [kafka.test.config :as config]
   [kafka.test.zk :as zk]
   [kafka.test.fs :as fs]
   [kafka.test.kafka :as kafka]
   [manifold.stream :as s]
   [manifold.deferred :as d]
   [com.stuartsierra.component :as component])
  (:import
   (java.util.concurrent CountDownLatch)
   (org.I0Itec.zkclient ZkClient ZkConnection)
   (kafka.server KafkaConfig KafkaServerStartable)
   (kafka.admin AdminUtils)
   (kafka.utils ZkUtils ZKStringSerializer$)
   (java.net InetSocketAddress)

   (org.apache.zookeeper.server ZooKeeperServer ServerCnxnFactory)
   (org.apache.kafka.clients.consumer ConsumerRecord)
   (org.apache.kafka.clients.producer ProducerRecord)))

(defn start!
  "Starts the test harness

   Returns a modified test harness with the following slots

     :producer    a kafka producer with the supplied config
     :zk-utils    this is required for administrive tasks like
                  creating/listing topics etc
     :zk-client   internal zk client for things like creating
                  test topics, get broker listing etc"
  [{:keys [config] :as harness}]
  (let [zk-client      (zk/client (get-in config [:broker]))
        zk-utils       (zk/utils zk-client)

        producer       (kafka/producer
                        (config/props config [:producer]))]

    {:producer producer
     :zk-utils zk-utils
     :zk-client zk-client}))


(defn stop!
  "Stops the test harness"
  [{:keys [log-streams zk-client producer]}]
  (do
    (doseq [log-stream @log-streams]
      (s/close! log-stream))

    (doseq [closeable [producer zk-client]]
      (when closeable
        (.close closeable)))

    {:producer nil
     :zk-utils nil
     :zk-client nil}))

(defn put!
  "Put a record onto a kafka topic"
  ([{:keys [producer]} {:keys [topic key value]}]
   @(.send producer (if key
                      (ProducerRecord. topic key value)
                      (ProducerRecord. topic value))))

  ([{:keys [producer]} {:keys [topic key value]} callback-fn]
  (let [record (if key
                 (ProducerRecord. topic key value)
                 (ProducerRecord. topic value))]
    @(.send producer record callback-fn))))

(defn logs
  "Return a stream of logs for the supplied topics.

   The stream will be closed (and its resources released) when the harness
   is stopped"
  [{:keys [config log-streams]} topics]
  (let [consumer (kafka/consumer
                  (config/props config [:consumer])
                  topics)
        log (s/stream)]

    (.subscribe consumer topics)
    (s/on-closed log #(locking consumer (.close consumer)))
    (swap! log-streams conj log)

    (s/consume (fn [rec]
                  (s/put! log
                          {:topic (.topic rec)
                           :key (.key rec)
                           :value (.value rec)}))
                (kafka/log-seq consumer))
    log))

(defrecord Harness [config zk-utils zk-client log-streams producer]
  component/Lifecycle
  (start [this]
    (merge this (start! this)))

  (stop [this]
    (merge this (stop! this))))

(defn harness [config]
  (map->Harness {:config config
                 :log-streams (atom [])}))

;; Experimental
;;
;; The idea here is to make the test interface the same whether the test is
;; to be run locally *or* against a remote kafka cluster.
;;
;; So we have an "embedded" test system that starts up it's own zookeeper and
;; kafka services before running the tests. But in addition, we have the "remote"
;; test system that just starts up the harness. We can control which one is
;; invoked just by making a different config available on jenkins or whatever.

(defn multi-broker [n config]
  (assoc config
         "broker.id" (str n)
         "port" (str (+ n 9092))
         "log.dirs" (fs/tmp-dir "kafka-log" (str n))))

(defn harness-system [config]
  (component/system-map
   :zookeeper (zk/server (:broker config))
   :kafka (component/using
           (kafka/server (:broker config))
           [:zookeeper])
   :harness (component/using
             (harness config)
             [:kafka])))

(defmulti test-harness #(get % "harness"))

(defmethod test-harness "embedded" [config]
  (component/system-map
   :zookeeper (zk/server (:broker config))
   :kafka (component/using
           (kafka/server (:broker config))
           [:zookeeper])
   :harness (component/using
             (harness config)
             [:kafka])))

(defmethod test-harness "multi-broker" [config]
  (component/system-map
   :zookeeper (zk/server (:broker config))

   :kafka-1 (component/using
             (kafka/server (multi-broker 1 (:broker config)))
             [:zookeeper])
   :kafka-2 (component/using
             (kafka/server (multi-broker 2 (:broker config)))
             [:zookeeper])
   :kafka-3 (component/using
             (kafka/server (multi-broker 3 (:broker config)))
             [:zookeeper])

   :harness (component/using
             (harness config)
             [:kafka-1 :kafka-2 :kafka-3 :zookeeper])))

(defmethod test-harness "remote" [config]
  (component/system-map
   :harness (harness config)))

(defn try-start
  "Tries to start `system` but stops it if there is an error
   during the `start-system` method"
  [system]
  (try
    (swap! system component/start-system)
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [component system]} (ex-data e)]
        (component/stop-system system)
        (throw e)))))

;; Not ready yet
(defn call-with-test-harness
  [run-test-with config]
  (let [harness (atom (test-harness config))]
    (try-start harness)
    (try
      (run-test-with (:harness @harness))
      (finally
        (swap! harness component/stop-system)))))

(defmacro with-test-harness [[sym config] & body]
  `(call-with-test-harness
    (fn [harness#]
      (let [~sym harness#]
        ~@body))
    ~config))
