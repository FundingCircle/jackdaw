(ns kafka.test.fixtures
  "Test fixtures for kafka based apps"
  (:require
   [clojure.tools.logging :as log]
   [kafka.admin :as admin]
   [kafka.core :as kafka]
   [kafka.test.zk :as zk]
   [kafka.test.kafka :as broker]
   [environ.core :refer [env]])
  (:import
   (kafka.common TopicExistsException)
   (java.util.concurrent CountDownLatch LinkedBlockingQueue)
   (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
   (org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback)))

(def ^:dynamic *zk-utils*)
(def ^:dynamic *consumers*)
(def ^:dynamic *loggers*)

;; utils

(defn embedded-fixtures? []
  (read-string (env :embedded-fixtures)))

(defn latch [n]
  (CountDownLatch. n))

;; services

(defn zookeeper
  "A zookeeper test fixture

   Start up a zookeeper broker with the supplied config before running
   the test `t`"
  [config]
  (fn [t]
    (if-not (embedded-fixtures?)
      (t)
      (let [zk (zk/start! {:config config})]
        (try
          (t)
          (finally
            (zk/stop! zk)))))))

(defn broker
  "A kafka test fixture.

   Start up a kafka broker with the supplied config before running the
   test `t`"
  [config]
  (when-not (get config "log.dirs")
    (throw (ex-info "Invalid config: missing required field 'log.dirs'"
                    {:config config})))

  (fn [t]
    (if-not (embedded-fixtures?)
      (t)
      (let [log-dirs (get config "log.dirs")
            kafka (broker/start! {:config config})]
        (try
          (t)
          (finally
            (broker/stop! (merge kafka {:log-dirs log-dirs}))))))))

;; auto-closing client

(defn zk-utils [config]
  (fn [t]
    (let [client (zk/client config)
          utils (zk/utils client)]
      (binding [*zk-utils* utils]
        (try
          (t)
          (finally
            (.close client)))))))

;; basic auto-topic creation/deletion

(defn with-topics [config topics]
  "During development, this seemed useful. Requires the zk-utils fixture"
  (fn [t]
    (try
      (doseq [topic topics]
        (try
          (admin/create-topic! *zk-utils* topic 1 1 {})
          (catch TopicExistsException e
            (log/info "topic already exists: " topic))))
      (t)
      (finally
        (doseq [topic topics]
          (admin/delete-topic! *zk-utils* topic))))))

;; Producer Registry
;;
;;  Introducing the concept of a producer registry allows us to create a fixture
;;  that loads a "registry" with all producers the test might want to use, then
;;  when tests want to publish! they can reference the producer it wants by a
;;  keyword id.
;;
;;

(def ^:dynamic *producers*)

(defn- open-producers [configs]
  (let [logger (fn [p]
                 (log/infof "opened producer: %s" (first p)))]
    (->> (for [[k cfg] configs]
           [k (kafka/producer cfg)])
         (mapcat identity)
         (apply hash-map))))

(defn- close-producers []
  (doseq [[k p] *producers*]
    (.close p)
    (log/info "closed producer: %s" k)))

(defn producers [configs]
  (fn [t]
    (binding [*producers* (open-producers configs)]
      (try
        (t)
        (finally
          (close-producers))))))

(defn publish!
  "Identifying the producer by :id means we can delegate the clean up
   of producers after the test to the fixture"
  ([id {:keys [topic key value]}]
   (let [producer (get *producers* id)
         record (if key
                  (ProducerRecord. topic key value)
                  (ProducerRecord. topic value))]
     (.send producer record))))

;; Consumer Registry
;;
;;  Same as above

(defn open-consumers [configs]
  (->> (for [[k cfg] configs]
         [k (do
              (doto (kafka/consumer cfg)
                (.subscribe [(name k)])))])
       (mapcat identity)
       (apply hash-map)))

(defn find-consumer [id]
  (get *consumers* id))

(defn close-consumers []
  (doseq [[k p] *consumers*]
    (.close p)))

;; Logger Registry
;;
;; The consumer abstraction is a bit lower-level than we'd like for tests so "loggers"
;; automatically poll the consumers in their own thread and provide a lazy-seq over
;; the results

(defn- lazy-iterate
  [it]
  (lazy-seq
   (when (.hasNext it)
     (cons (.next it) (lazy-iterate it)))))

(defn- lazy-polling
  [consumer latch]
  (lazy-seq
   (when-let [records (when (pos? (.getCount latch))
                        (locking consumer (.poll consumer 100)))]
     (concat (lazy-iterate (.iterator records))
             (when (pos? (.getCount latch))
               (lazy-polling consumer latch))))))

(defn logger [k cfg latch]
  (let [consumer (find-consumer k)
        queue (LinkedBlockingQueue. 100)
        processor (future
                    (loop []
                      (let [stop? (zero? (.getCount latch))
                            records (when-not stop?
                                      (locking consumer (.poll consumer 1000)))]
                        (when-not stop?
                          (when (pos? (.count records))
                            (doseq [rec (iterator-seq (.iterator records))]
                              (.put queue rec)))
                          (recur)))))]
    {:queue queue
     :processor processor}))

(defn open-loggers [latch configs]
  (->> (for [[k cfg] configs]
         [k (logger k cfg latch)])
       (mapcat identity)
       (apply hash-map)))

(defn close-consumers []
  (doseq [[k c] *consumers*]
    (.close c)))

(defn close-loggers [latch]
  (.countDown latch)
  (doseq [[k {:keys [processor]}] *loggers*]
    @processor))

(defn consumers [configs]
  (fn [t]
    (binding [*consumers* (open-consumers configs)]
      (try
        (t)
        (finally
          (close-consumers))))))

(defn loggers [configs]
  (fn [t]
    (let [latch (latch 1)]
      (binding [*loggers* (open-loggers latch configs)]
        (try
          (t)
          (finally
            (close-loggers latch)))))))

(defn lazy-logs [id]
  (lazy-seq
   (let [queue (get-in *loggers* [id :queue])]
     (when-let [item (.take queue)]
       (cons item (lazy-logs id))))))
