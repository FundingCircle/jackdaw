(ns kafka.core
  "Clojure wrapper to kafka consumers/producers"
  (:import
   (org.apache.kafka.clients.producer KafkaProducer Callback)
   (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRebalanceListener)
   (org.apache.kafka.clients.consumer ConsumerRecord)
   (org.apache.kafka.clients.producer ProducerRecord)))

(defn producer
  "Return a KafkaProducer with the supplied properties"
  ([config]
   (KafkaProducer. config))

  ([config key-serializer value-serializer]
   (KafkaProducer. config key-serializer value-serializer)))

(defn consumer
  "Return a KafkaConsumer with the supplied properties"
  ([props]
   (KafkaConsumer. props))

  ([props key-deserializer value-deserializer]
   (KafkaConsumer. props key-deserializer value-deserializer)))

(defn logs
  "Return a lazy-seq with all records delivered by kafka to the supplied consumer.

   Provided as a convenience so we don't have to manually poll a consumer.

   An optional latch may be used to ensure no poll occurs after releasing
   the latch. This can be useful when the consumer may be closed in another
   thread."
  ([consumer]
   (logs nil consumer 1000))

  ([consumer latch]
   (logs latch consumer 1000))

  ([consumer latch poll-freq-ms]
   (letfn [(lazy-iterate [iterator]
             (lazy-seq
              (when (.hasNext iterator)
                (cons (.next iterator) (lazy-iterate iterator)))))]

     (lazy-seq
      (when-let [records (when (and latch (pos? (.getCount latch)))
                           (locking consumer (.poll consumer poll-freq-ms)))]
        (concat (lazy-iterate
                 (.iterator records))
                (logs consumer latch poll-freq-ms)))))))
