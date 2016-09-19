(ns kafka.test.kafka
  (:require
   [clojure.java.io :as io]
   [manifold.stream :as s]
   [manifold.deferred :as d]
   [kafka.test.config :as config]
   [kafka.test.fs :as fs]
   [com.stuartsierra.component :as component])
  (:import
   (java.util.concurrent CountDownLatch)
   (kafka.admin AdminUtils)
   (kafka.utils ZkUtils ZKStringSerializer$)
   (kafka.server KafkaConfig KafkaServerStartable)
   (org.apache.kafka.clients.producer KafkaProducer Callback)
   (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRebalanceListener)
   (org.apache.zookeeper.server ZooKeeperServer ServerCnxnFactory)
   (org.apache.kafka.clients.consumer ConsumerRecord)
   (org.apache.kafka.clients.producer ProducerRecord)))

(defn start!
  "Starts a kakfa broker.

   Returns a map containing the broker instance itself and a latch
   that waits until the broker is shutdown"
  [{:keys [config]}]
  (let [broker (-> config
                   (config/props)
                   (KafkaConfig.)
                   (KafkaServerStartable.))
        latch (CountDownLatch. 1)]
    (.startup broker)
    {:broker broker
     :latch latch}))

(defn stop!
  "Stops a kafka broker.

   Shuts down the broker, releases the latch, and deletes log files"
  [{:keys [broker latch log-dirs]}]
  (when broker
    (try
      (.shutdown broker)
      (.countDown latch)
      {:broker nil}
      (finally
        (fs/try-delete! (io/file log-dirs))))))

(defrecord Kafka [zookeeper config broker countdown]
  component/Lifecycle
  (start [this]
    (merge this (start! this)))

  (stop [this]
    (merge this (stop! this))))

(defn server
  "Return a kafka server as a lifecycle component"
  [config]
  (if-let [log-dirs (get config "log.dirs")]
    (map->Kafka {:config config,
                 :log-dirs log-dirs})
    (throw (ex-info "Invalid config: missing required field 'log.dirs'"
                    {:config config}))))

(defn callback
  "Build a kafka producer callback function out of a normal clojure one

   The function should expect two parameters, the first being the record
   metadata, the second being an exception if there was one. The function
   should check for an exception and handle it appropriately."
  [on-completion]
  (reify Callback
    (onCompletion [this record-metadata exception]
      (on-completion record-metadata exception))))

(defn producer
  "Build a kafka producer with the supplied config."
  [config]
  (KafkaProducer. config))

(defn consumer
  "Build a kafka consumer with the supplied properties

   There are two ways to call this

    1. With just the props supplied. Caller is responsible for managing
       their own subscription

    2. With props and topics supplied. The consumer will subscribe to
       the supplied topics

   I thought about adding another way that makes it easy to subscribe to
   some topic regex but that starts to make it more complicated and I'm
   not sure we'd even need it. A test will usually know which topics it
   is listening for."
  ([props]
   (KafkaConsumer. props))

  ([props topics]
   (let [consumer (KafkaConsumer. props)]
     (doto consumer
       (.subscribe topics)))))


(defn log-seq
  "Returns a lazy-seq representing all records delivered by kafka to
   the supplied consumer.

   An optional second parameter allows you to tweak the polling frequency. By
   default we poll every 1000 milliseconds"
  ([consumer]
   (log-seq consumer 1000))

  ([consumer poll-freq-ms]
   (letfn [(lazy-iterate [iterator]
             (lazy-seq
              (when (.hasNext iterator)
                (cons (.next iterator) (lazy-iterate iterator)))))]

     (lazy-seq
      (when-let [records (locking consumer (.poll consumer poll-freq-ms))]
        (concat (lazy-iterate
                 (.iterator records))
                (log-seq consumer poll-freq-ms)))))))
