(ns kafka.test.fixtures
  "Test fixtures for kafka based apps"
  (:require
   [clojure.test :as test]
   [clojure.tools.logging :as log]
   [kafka.admin :as admin]
   [kafka.client :as client]
   [kafka.test.config :as config]
   [kafka.test.zk :as zk]
   [kafka.test.kafka :as broker])
  (:import
   (kafka.common TopicExistsException)
   (java.util.concurrent CountDownLatch LinkedBlockingQueue)
   (io.confluent.kafka.schemaregistry.rest SchemaRegistryRestApplication SchemaRegistryConfig)
   (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
   (org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback)))

(def ^:dynamic *zk-utils*)
(def ^:dynamic *consumer-registry*)
(def ^:dynamic *producer-registry*)
(def ^:dynamic *log-seq-registry*)

;; utils

(defn latch [n]
  (CountDownLatch. n))

(defn queue [n]
  (LinkedBlockingQueue. n))

;; services

(defn zookeeper
  "A zookeeper test fixture

   Start up a zookeeper broker with the supplied config before running
   the test `t`"
  [config]
  (fn [t]
    (let [zk (zk/start! {:config config})]
      (try
        (log/info "Started zookeeper fixture" zk)
        (t)
        (finally
          (zk/stop! zk)
          (log/info "Stopped zookeeper fixture" zk))))))

(defn broker
  "A kafka test fixture.

   Start up a kafka broker with the supplied config before running the
   test `t`"
  [config]
  (when-not (get config "log.dirs")
    (throw (ex-info "Invalid config: missing required field 'log.dirs'"
                    {:config config})))

  (fn [t]
    (let [log-dirs (get config "log.dirs")
          kafka (broker/start! {:config config})]
      (try
        (log/info "Started kafka fixture" kafka)
        (t)
        (finally
          (broker/stop! (merge kafka {:log-dirs log-dirs}))
          (log/info "Stopped kafka fixture" kafka))))))

(defn multi-broker
  "A multi-broker kafka fixture

   Starts up `n` kafka brokers by generating a vector of configs
   by invoking the `multi-config` function on each integer up to `n`."
  [config n]
  (fn [t]
    (let [multi-config (config/multi-config config)
          configs (map multi-config (range n))
          cluster (doall (map (fn [cfg]
                                (assoc (broker/start! {:config cfg})
                                       :log-dirs (get cfg "log.dirs")))
                              configs))]
      (try
        (log/info "Started multi-broker fixture" cluster)
        (t)
        (finally
          ;; This takes a surprisingly
          (doseq [node (reverse cluster)]
            (broker/stop! node))
          (log/info "Stopped multi-broker fixture" cluster))))))

(defn schema-registry
  [config]
  (fn [t]
    (let [app (SchemaRegistryRestApplication.
               (SchemaRegistryConfig. (config/properties config)))
          server (.createServer app)]
      (try
        (.start server)
        (log/info "Started schema registry fixture" server)
        (t)
        (finally
          (.stop server)
          (log/info "Stopped schema registry fixture" server))))))

;; auto-closing client

(defn zk-utils
  "An instance of ZkUtils (typically required by kafka admin for ops)

   Closes the corresponding zk client after the tests"
  [config]
  (fn [t]
    (let [client (zk/client config)
          utils (zk/utils client)]
      (binding [*zk-utils* utils]
        (try
          (t)
          (finally
            (.close client)))))))

;; Producer Registry
;;
;;  Introducing the concept of a producer registry allows us to create a fixture
;;  that loads a "registry" with all producers the test might want to use, then
;;  when tests want to publish! they can reference the producer they want by a
;;  keyword id.
;;
;;

(defn- open-producers [configs]
  (let [logger (fn [p]
                 (log/infof "opened producer: %s" (first p)))]
    (->> (for [[k cfg] configs]
           [k (cond
                (map? cfg)    (client/producer cfg (str (name k)))
                (vector? cfg) (let [[cfg key-serde val-serde] cfg]
                                (client/producer cfg key-serde val-serde (str (name k))))
                :else         (throw (ex-info "unsupported producer config"
                                              {:producer k
                                               :config cfg})))])
         (into {}))))

(defn- close-producers []
  (doseq [[k p] *producer-registry*]
    (.close p)))

(defn producer-registry [configs]
  (fn [t]
    (binding [*producer-registry* (open-producers configs)]
      (try
        (log/info "Started producer registry fixture")
        (t)
        (finally
          (close-producers)
          (log/info "Stopped producer registry fixture"))))))

(defn find-producer [id]
  (get *producer-registry* id))

;; Consumer Registry
;;
;;  Same as above

(defn open-consumers [configs]
  (->> (for [[k cfg] configs]
         [k (let [consumer (cond
                             (map? cfg)    (client/consumer cfg (str (name k)))
                             (vector? cfg) (let [[cfg key-serde val-serde] cfg]
                                             (client/consumer cfg key-serde val-serde (str (name k))))
                             :else (throw (ex-info "unsupported consumer config"
                                                   {:consumer k
                                                    :config cfg})))]
              consumer)])
       (mapcat identity)
       (apply hash-map)))

(defn find-consumer [id]
  (get *consumer-registry* id))

(defn- close-consumers
  "Close any opened consumers"
  []
  (doseq [[k c] *consumer-registry*]
    (.close c)))

(defn consumer-registry
  "Open a collection of consumers.

   Consumers are quite low-level. See `loggers` for an API that is a bit
   easier to use for writing tests."
  [configs]
  (fn [t]
    (binding [*consumer-registry* (open-consumers configs)]
      (try
        (log/info "Started consumer registry")
        (t)
        (finally
          (close-consumers)
          (log/info "Stopped consumer registry"))))))

;; fixture composition

(defn identity-fixture
  "They have this already in clojure.test but it is called
   `default-fixture` and it is private. Probably stu seirra's fault
   :troll:"
  [t]
  (t))

(defn kafka-platform
  "Combine the zookeeper, broker, and schema-registry fixtures into
   one glorius test helper"
  [zk-config broker-config embedded?]
  (if embedded?
    (test/compose-fixtures (zookeeper zk-config)
                           (broker broker-config))
    identity-fixture))
