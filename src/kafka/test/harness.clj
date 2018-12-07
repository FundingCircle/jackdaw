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


(defn collect-logs! [{:keys [config topics latch log-stream]}]
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


(defn start! [{:keys [config topics] :as harness}]
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


(defn stop! [{:keys [log-stream zk-client producer latch]}]
  (.countDown latch)
  (s/close! log-stream)

  (doseq [closeable [producer zk-client]]
    (when closeable
      (.close closeable)))

  {:producer nil
   :consumer nil
   :log-stream nil})

(defn put! [{:keys [producer]} {:keys [topic key value]} callback-fn]
  (let [record (if key
                 (ProducerRecord. topic key value)
                 (ProducerRecord. topic value))]
    @(.send producer record callback-fn)))

(defn take! [{:keys [log-stream]}]
  (s/take! log-stream))

;;
;; The test harness starts up a kafka producer with which to publish test
;; messages and a consumer subscribed to all topics and `put!`s them
;; on to a manifold stream which tests can consume to make assertions
;; about the expected output of one or more stream processors.
;;
;; We use a java.util.concurrent.CountDownLatch to wait for the tests to
;; finished and `close` the

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

;; (defn try-start
;;   "Tries to start `system` but stops it if there is an error
;;    during the `start-system` method"
;;   [system]
;;   (try
;;     (swap! system component/start-system)
;;     (catch clojure.lang.ExceptionInfo e
;;       (let [{:keys [component system]} (ex-data e)]
;;         (component/stop-system system)
;;         (throw e)))))

;; ;;
;; ;; Always nice to build macros on top of a functional interface. At the
;; ;; least, easier to debug when things go wrong, but gives the user a
;; ;; bit more flexiblity with regard to usage.
;; ;;
;; ;; The way this works is that we start up the test harness, then run the
;; ;; provided test function. It's up to you to write a test function that
;; ;; doesn't block forever (e.g. if there's a chance your test might not
;; ;; produce the output it's expecting, you could use `try-take!`
;; (defn call-with-test-harness
;;   [run-test-with config]
;;   (let [harness (atom (test-harness config))]

;;     (try-start harness)

;;     (try
;;       (run-test-with (:harness @harness))
;;       (finally
;;         ;; countdown to release of the log collection thread
;;         (.countDown (get-in @harness [:harness :countdown]))
;;         (component/stop @harness)))))


;; (defmacro with-test-harness [[sym config] & body]
;;   `(call-with-test-harness
;;     (fn [harness#]
;;       (let [~sym harness#]
;;         ~@body))
;;     ~config))
