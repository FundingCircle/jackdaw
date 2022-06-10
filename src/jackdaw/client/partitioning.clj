(ns jackdaw.client.partitioning
  "Extras for `jackdaw.client` which help you define record partitioning
  schemes as part of a topic's configuration.

  The partitioning API provided by Kafka, like the serdes API, leaves
  a lot to be desired when trying to interop from Clojure. You have to
  define a `org.apache.kafka.clients.producer.Partitioner` class
  implementation with an 0-arity constructor, and you include the name
  of that Partitioner class in your producer options. This seems to
  have been done so that the Partitioner can have access to Kafka
  internal state about the cluster, from which to read partition count
  and related data. But this pretty soundly defeats Clojure's idioms
  of avoiding class generation wherever possible and using instance
  parameterization.

  The `Producer` (and `Consumer`) APIs however do expose
  `.partitionsFor` - a way to interrogate a topic to understand how
  many partitions it contains.

  This namespace defines a mechanism by which clients can define
  \"defaulting\" behavior both for record keys, and for record
  partitioning.

  Lets say I want to specify that my topic is always partitioned by
  some field of the records on the topic. It would be convenient to
  let a (thin) framework handle that.

  Likewise it would be convenient to easily define as normal Clojure
  functions the computation by which I wish to assign records to
  partitions, rather than having to code up a custom class.

  This namespace provides both capabilities via an extended
  `#'->ProducerRecord`, and provides a `#'produce!` identical to that
  in `jackdaw.client` but backed by the partitioning machinery."
  {:license
   "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.string :as str]
            [jackdaw.client :as jc]
            [jackdaw.data :as jd])
  (:import org.apache.kafka.clients.producer.Producer
           org.apache.kafka.common.serialization.Serde
           org.apache.kafka.common.utils.Utils))

(set! *warn-on-reflection* true)

(defn record-key->key-fn
  "Given a topic config having `:record-key`, parse it,
  annotating the topic with a `:Lkey-fn` which will simply fetch the
  specified record-key out of any record."
  [{:keys [record-key] :as t}]
  (let [record-key (as-> record-key %
                     (-> %
                         (str/replace  "$." "")
                         (str/replace  "_" "-")
                         (str/split  #"\."))
                     (mapv keyword %))]
    (assoc t ::key-fn #(get-in % record-key))))

(defn default-partitioner*
  "Mimics the kafka default partitioner."
  ^Integer [^bytes key-bytes ^Integer num-partitions]
  {:pre [(some? key-bytes) (pos? num-partitions)]}
  (-> key-bytes Utils/murmur2 Utils/toPositive (mod num-partitions) int))

(defn default-partition
  "The kafka default partitioner. As a `::partition-fn`"
  [{:keys [topic-name key-serde]} key _value partitions]
  (let [key-bytes (.serialize (.serializer ^Serde key-serde) topic-name key)]
    (default-partitioner* key-bytes partitions)))

(defn ->ProducerRecord
  "Like `jackdaw.records/->ProducerRecord`, but with partitioning support.

  When constructing a `ProducerRecord` for which no `key` is provided,
  `::key-fn` will be invoked with the record to be produced. Its
  return value will be used as the partitioning key.

  When constructing a `ProducerRecord` for which no `partition` is
  provided (even if the key is provided or was computed!) then
  `::partition-fn` will be called with the `::jt/topic`, key, value,
  and number of partitions.

  See `#'record-key->key-fn` for an example of annotating a topic with
  a `key-fn` function."
  ([^Producer producer {:keys [key-fn] :as t} value]
   (if key-fn
     (->ProducerRecord producer t (key-fn value) value)
     (jd/->ProducerRecord t value)))
  ([^Producer producer {:keys [partition-fn] :as t} key value]
   (if partition-fn
     (as-> (jc/num-partitions producer t) %
       (partition-fn t key value %)
       (->ProducerRecord producer t % key value))
     (jd/->ProducerRecord t key value)))
  ([^Producer _producer topic partition key value]
   (jd/->ProducerRecord topic (int partition) key value))
  ([^Producer _producer topic partition timestamp key value]
   (jd/->ProducerRecord topic partition  timestamp key value))
  ([^Producer _producer topic partition timestamp key value headers]
   (jd/->ProducerRecord topic partition timestamp key value headers)))

(defn produce!
  "Like `#'jackdaw.client/produce!` but used the partitioning machinery
  if possible rather than just building a `ProducerRecord`.

  Returns a future which will produce datafied record metadata when
  forced."
  ([producer topic value]
   (jc/send! producer
             (->ProducerRecord producer topic value)))
  ([producer topic _key value]
   (jc/send! producer
             (->ProducerRecord producer topic value)))
  ([producer topic partition _key value]
   (jc/send! producer
             (->ProducerRecord producer topic partition topic value)))
  ([producer topic partition timestamp _key value]
   (jc/send! producer
             (->ProducerRecord producer topic partition timestamp topic value)))
  ([producer topic partition timestamp _key value headers]
   (jc/send! producer
             (->ProducerRecord producer topic partition timestamp topic value headers))))
