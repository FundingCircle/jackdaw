(ns jackdaw.test.transports.kafka
  (:require
   [clojure.tools.logging :as log]
   [clojure.stacktrace :as stacktrace]
   [manifold.stream :as s]
   [manifold.deferred :as d]
   [jackdaw.client :as kafka]
   [jackdaw.data :as jd]
   [jackdaw.test.journal :as j]
   [jackdaw.test.transports :as t :refer [deftransport]]
   [jackdaw.test.serde :refer [apply-serializers apply-deserializers
                               serde-map
                               byte-array-serde]])
  (:import
   org.apache.kafka.streams.KafkaStreams$StateListener
   org.apache.kafka.clients.consumer.ConsumerRecord
   org.apache.kafka.clients.producer.ProducerRecord))

(defn subscribe
  "Subscribes to specified topics

   `consumer` should be a kafka consumer
   `topic-config` should be a sequence of topic-metadata maps"
  [consumer topic-config]
  (kafka/subscribe consumer topic-config))

(defn load-assignments
  [consumer]
  (.poll consumer 0)
  (.assignment consumer))

(defn seek-to-end
  "Seeks to the end of all the partitions assigned to the given consumer
   and returns the updated consumer"
  [consumer & topic-partitions]
  (let [assigned-partitions (or topic-partitions (load-assignments consumer))]
    (.seekToEnd consumer assigned-partitions)
    (doseq [assigned-partition assigned-partitions]
      ;; This forces the seek to happen now
      (.position consumer assigned-partition))
    consumer))

(defn poller
  "Returns a function that takes a consumer and puts any messages retrieved
   by polling it onto the supplied `messages` channel"
  [messages]
  (fn [consumer]
    (try
      (let [m (.poll consumer 1000)]
        (when m
          (s/put-all! messages m)))
      (catch Throwable e
        (log/error (Throwable->map e))
        (s/put! messages {:error e})))))

(defn subscription
  "Subscribes to `topic-collection` and seeks to the end of all partitions. This
   is usually what you want in a testing context. It's best for the test you're
   trying to run now to ignore all the garbage created by previous tests."
  [kafka-config topic-collection]
  (-> (kafka/consumer kafka-config byte-array-serde)
      (subscribe topic-collection)
      (seek-to-end)))

(defn mk-consumer-record
  "Clojurize the ConsumerRecord returned from consuming a kafka record"
  [^ConsumerRecord consumer-record]
  (when consumer-record
    {:checksum (.checksum consumer-record)
     :key (.key consumer-record)
     :offset (.offset consumer-record)
     :partition (.partition consumer-record)
     :serializedKeySize (.serializedKeySize consumer-record)
     :serializedValueSize (.serializedValueSize consumer-record)
     :timestamp (.timestamp consumer-record)
     :topic (.topic consumer-record)
     :value (.value consumer-record)}))

(defn ^ProducerRecord mk-producer-record
  "Creates a kafka ProducerRecord for use with `send!`."
  ([{:keys [topic-name]} value]
   (ProducerRecord. ^String topic-name value))
  ([{:keys [topic-name]} key value]
   (ProducerRecord. ^String topic-name key value))
  ([{:keys [topic-name]} partition key value]
   (ProducerRecord. ^String topic-name ^Integer (int partition) key value))
  ([{:keys [topic-name]} partition timestamp key value]
   (ProducerRecord. ^String topic-name ^Integer (int partition) ^Long timestamp key value)))

(defn consumer
  "Creates an asynchronous Kafka Consumer of all topics defined in the
   supplied `topic-metadata`

   Puts all messages on the channel in the returned response. It is the
   responsibility of the caller to arrange for the read the channel to
   be read by some other process.

   Must be closed with `close-consumer` when no longer required"
  [kafka-config topic-metadata deserializers]
  (let [continue?   (atom true)
        messages    (s/stream 1 (comp
                                 (map #'mk-consumer-record)
                                 (map #(apply-deserializers deserializers %))
                                 (map #(assoc % :topic (j/reverse-lookup topic-metadata (:topic %))))))
        started?    (promise)
        poll        (poller messages)]

    {:process (d/loop [consumer (subscription kafka-config
                                              (vals topic-metadata))]
                (d/chain (d/future consumer)
                         (fn [c]
                           (when-not (realized? started?)
                             (deliver started? true)
                             (log/infof "started kafka consumer: %s"
                                        (select-keys kafka-config ["bootstrap.servers" "group.id"])))

                           (if @continue?
                             (do (poll consumer)
                                 (d/recur consumer))
                             (do
                               (s/close! messages)
                               (.close consumer)
                               (log/infof "stopped kafka consumer: %s"
                                          (select-keys kafka-config ["bootstrap.servers" "group.id"])))))))
     :started? started?
     :messages messages
     :continue? continue?}))

(defn close-consumer
  [consumer]
  (reset! (:continue? consumer) false)
  (deref (:process consumer)))

(defn build-record
  "Builds a Kafka Producer and assoc it onto the message map"
  [m]
  (let [rec (mk-producer-record (:topic m)
                                (:partition m (int 0))
                                (:timestamp m)
                                (:key m)
                                (:value m))]
    (assoc m :producer-record rec)))

(defn deliver-ack
  "Deliver the `ack` promise with the result of attempting to write to kafka. The
   default command-handler waits for this before on to the next command so the
   test response may indicate the success/failure of each write command."
  [ack]
  (fn [rec-meta ex]
    (when-not (nil? ack)
      (if ex
        (deliver ack {:error ex})
        (deliver ack (select-keys (jd/datafy rec-meta)
                                  [:topic-name :offset :partition
                                   :serialized-key-size
                                   :serialized-value-size]))))))


(defn producer
  "Creates an asynchronous kafka producer to be used by a test-machine for for
   injecting test messages"
  ([kafka-config topic-config serializers]
   (let [producer       (kafka/producer kafka-config byte-array-serde)
         messages       (s/stream 1 (map (fn [x]
                                           (try
                                             (-> (apply-serializers serializers x)
                                                 (build-record))
                                             (catch Exception e
                                               (let [trace (with-out-str
                                                             (stacktrace/print-cause-trace e))]
                                                 (log/error e trace))
                                               (assoc x :serialization-error e))))))

         _ (log/infof "started kafka producer: %s"
                      (select-keys kafka-config ["bootstrap.servers" "group.id"]))
         process (d/loop [message (s/take! messages)]
                   (d/chain (d/future message)
                     (fn [{:keys [producer-record ack serialization-error] :as m}]
                       (cond
                         serialization-error   (do (deliver ack {:error :serialization-error
                                                                 :message (.getMessage serialization-error)})
                                                   (d/recur (s/take! messages)))

                         producer-record       (do (kafka/send! producer producer-record (deliver-ack ack))
                                                   (d/recur (s/take! messages)))

                         :else (do
                                 (.close producer)
                                 (log/infof "stopped kafka producer: "
                                            (select-keys kafka-config ["bootstrap.servers" "group.id"])))))))]

     {:producer  producer
      :messages  messages
      :process   process})))

(deftransport :kafka
  [{:keys [config topics]}]
  (let [serdes        (serde-map topics)
        test-consumer (consumer config topics (get serdes :deserializers))
        test-producer (when @(:started? test-consumer)
                        (producer config topics (get serdes :serializers)))]
    {:consumer test-consumer
     :producer test-producer
     :serdes serdes
     :topics topics
     :exit-hooks [(fn []
                    (s/close! (:messages test-producer)))
                  (fn []
                    (reset! (:continue? test-consumer) false)
                    @(:process test-consumer)
                    @(:process test-producer))]}))
