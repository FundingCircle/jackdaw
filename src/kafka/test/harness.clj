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

(defn count-down-latch
  "Provides the facility to wait for something to happen.

   In this case we use it as a signal to cleanup resources when we're
   done with the test harness"
  [n]
  (CountDownLatch. n))


(defn collect-logs!
  "Creates a kafka consumer and `put!`s all messages into the supplied
   `log-stream`.

   Blocks until the `latch` has been released."
  [{:keys [config topics latch log-stream]}]
  (let [consumer (kafka/consumer
                  (config/props config [:consumer])
                  topics)]

    (s/on-closed log-stream #(locking consumer (.close consumer)))

    (s/consume (fn [rec]
                 (s/put! log-stream {:topic (.topic rec)
                                     :key (.key rec)
                                     :value (.value rec)}))
               (kafka/log-seq consumer))
    (.await latch)))


(defn start!
  "Starts the test harness

   Returns a modified test harness with the following slots

     :producer    a kafka producer with the supplied config
     :zk-utils    this is required for administrive tasks like
                  creating/listing topics etc
     :log-stream  a manifold stream into which all records consumed
                  by the consumer are `put!`
     :latch       a java.util.concurrent primitive to indicate
                  when the consumer can be closed
     :collector   the log collection thread. this is a manifold
                  future that can be dereferenced once the consumer
                  has been closed."
  [{:keys [config topics] :as harness}]
  (let [acks           (atom [])
        offsets        (atom {})

        log-stream     (s/stream)
        zk-client      (zk/client (get-in config [:broker]))
        zk-utils       (zk/utils zk-client)
        stopped?       (d/deferred)
        latch          (count-down-latch 1)

        producer       (kafka/producer
                        (config/props config [:producer]))

        try-collect!   (fn []
                         (try
                           (collect-logs! (assoc harness
                                                 :topics topics
                                                 :latch latch
                                                 :log-stream log-stream))
                           (d/success! stopped? :ok)
                           (catch Exception e
                             (d/error! stopped? e))))]

    {:producer producer
     :zk-utils zk-utils
     :zk-client zk-client
     :log-stream log-stream
     :latch latch
     :stopped? stopped?
     :collector (d/future (try-collect!))}))


(defn stop!
  "Stops the test harness"
  [{:keys [log-stream zk-client producer latch]}]
  (.countDown latch)
  (s/close! log-stream)

  (doseq [closeable [producer zk-client]]
    (when closeable
      (.close closeable)))

  {:producer nil
   :consumer nil
   :log-stream nil})

(defn put!
  "Put a record onto a kafka topic"
  [{:keys [producer]} {:keys [topic key value]} callback-fn]
  (let [record (if key
                 (ProducerRecord. topic key value)
                 (ProducerRecord. topic value))]
    @(.send producer record callback-fn)))

(defn take!
  "Take a record from the subscription"
  [{:keys [log-stream]}]
  (s/take! log-stream))

(defrecord Harness [zk-utils zk-client
                    log-stream
                    latch stopped?
                    producer]
  component/Lifecycle
  (start [this]
    (merge this (start! this)))

  (stop [this]
    (merge this (stop! this))))

(defn harness [config]
  (map->Harness {:config config
                 :topics (:topics config)}))

;; Experimental
;;
;; The idea here is to make the test interface the same whether the test is
;; to be run locally *or* against a remote kafka cluster.
;;
;; So we have an "embedded" test system that starts up it's own zookeeper and
;; kafka services before running the tests. But in addition, we have the "remote"
;; test system that just starts up the harness. We can control which one is
;; invoked just by making a different config available on jenkins or whatever.

(defn harness-system [config]
  (component/system-map
   :zookeeper (zk/server config)
   :kafka (component/using
           (kafka/server config)
           [:zookeeper])
   :harness (component/using
             (harness config)
             [:kafka :zookeeper])))

(defmulti test-harness #(get % "harness"))

(defmethod test-harness "embedded" [config]
  (component/system-map
   :zookeeper (zk/server (:broker config))
   :kafka (component/using
           (kafka/server (:broker config))
           [:zookeeper])
   :harness (component/using
             (harness config)
             [:kafka :zookeeper])))

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
