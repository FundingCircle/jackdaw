(ns kafka.test.fixtures
  "Test fixtures for kafka based apps"
  (:require
   [clojure.test :as test]
   [clojure.tools.logging :as log]
   [kafka.admin :as admin]
   [kafka.client :as client]
   [kafka.test.zk :as zk]
   [kafka.test.kafka :as broker]
   [environ.core :refer [env]])
  (:import
   (kafka.common TopicExistsException)
   (java.util.concurrent CountDownLatch LinkedBlockingQueue)
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
        (t)
        (finally
          (zk/stop! zk))))))

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
        (t)
        (finally
          (broker/stop! (merge kafka {:log-dirs log-dirs})))))))

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

;; basic auto-topic creation/deletion

(defn with-topics [config topics]
  "During development, this seemed useful.

   Requires the zk-utils fixture"
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

(defn- open-producers [configs]
  (let [logger (fn [p]
                 (log/infof "opened producer: %s" (first p)))]
    (->> (for [[k cfg] configs]
           [k (client/producer cfg)])
         (mapcat identity)
         (apply hash-map))))

(defn- close-producers []
  (doseq [[k p] *producer-registry*]
    (.close p)))

(defn producer-registry [configs]
  (fn [t]
    (binding [*producer-registry* (open-producers configs)]
      (try
        (t)
        (finally
          (close-producers))))))

(defn publish!
  "Identifying the producer by :id means we can delegate the clean up
   of producers after the test to the fixture"
  ([id {:keys [topic key value]}]
   (let [producer (get *producer-registry* id)
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
              (doto (client/consumer cfg)
                (.subscribe [(name k)])))])
       (mapcat identity)
       (apply hash-map)))

(defn find-consumer [id]
  (get *consumer-registry* id))

(defn close-consumers []
  (doseq [[k p] *consumer-registry*]
    (.close p)))

;; Logger Registry
;;
;; The consumer abstraction is a bit lower-level than we'd like for tests so "loggers"
;; automatically poll the consumers in their own thread and provide a lazy-seq over
;; the results

(defn consumer-loop [consumer queue latch]
  (future
    (loop []
      (let [stop? (zero? (.getCount latch))
            records (when-not stop?
                      (locking consumer (.poll consumer 1000)))]
        (when-not stop?
          (when (pos? (.count records))
            (doseq [rec (iterator-seq (.iterator records))]
              (.put queue rec)))
          (recur))))))

(defn- logger
  "Can anyone think of a better name for this?

   The idea is to find the consumer `k` and feed all records read
   from it through a LinkedBlockingQueue.

   The consumer code originally used code from tubelines but we wanted more
   control over exactly when to stop consuming. A manual loop gives us a chance
   between each `.poll` to stop if the latch has been counted down to zero."
  [k cfg latch]
  (let [consumer (find-consumer k)
        queue (queue 100)
        processor (consumer-loop consumer queue latch)]
    {:queue queue
     :processor processor}))

(defn- open-loggers
  "Opens (starts?) a collection of loggers.

   Each logger has a `group-id` and consumes messages from it's configured topic.
   All threads will stop when the latch is released."
  [latch configs]
  (->> (for [[k cfg] configs]
         [k (logger k cfg latch)])
       (mapcat identity)
       (apply hash-map)))

(defn- close-loggers
  "Countdown the latch that stops the logger threads"
  [latch]
  (.countDown latch)
  (doseq [[k {:keys [processor]}] *log-seq-registry*]
    @processor))

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
        (t)
        (finally
          (close-consumers))))))

(defn log-seq-registry
  "Open a collection of loggers.

   Each logger finds a consumer with the same name, and for each record
   cosumed, inserts it into a java LinkedBlockingQueue. You can obtain
   a lazy-seq view of this queue by calling the function `log-seq`
   with the corresponding `group-id`.

   The hope is that mapping the data back into an ordinary clojure
   data structure will give people the freedom to write clean tests."
  [configs]
  (fn [t]
    (let [latch (latch 1)]
      (binding [*log-seq-registry* (open-loggers latch configs)]
        (try
          (t)
          (finally
            (close-loggers latch)))))))

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
  [broker-config]
  (if (read-string (env :embedded-fixtures))
    (test/compose-fixtures (zookeeper broker-config)
                           (broker broker-config))
    identity-fixture))

(defn log-seqs
  "Compose the `consumers` and `loggers` fixtures to provide the
   `log-seq` abstraction.

   Configs is a map of keywords to consumer parameters. If parameters
   is just a single map, we use the single arity Consumer constructor.
   Otherwise we pass parameters directly to the Consumer constructor.

   Enable this fixture if you want to be able to access a topic's
   output as a clojure lazy-seq. The can be obtained using"
  [configs]
  (test/compose-fixtures (consumer-registry configs)
                         (log-seq-registry configs)))

(defn log-seq
  "Get a lazy-sequence view of the kafka topic identified by `id`.

   Messages from the topic will be deserialized according to the
   the parameters you supplied to `log-seqs` in the test fixture."
  [id]
  (lazy-seq
   (let [queue (get-in *log-seq-registry* [id :queue])]
     (when-let [item (.take queue)]
       (cons item (log-seq id))))))
