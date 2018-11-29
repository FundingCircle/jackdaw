;; jackdaw.data.producer
;;
;; jackdaw.data extensions for the producer API types

(in-ns 'jackdaw.data)

(import '[org.apache.kafka.clients.producer
          ProducerRecord RecordMetadata]
        'org.apache.kafka.common.header.Headers)

;;; Producer record

(defn ^ProducerRecord ->ProducerRecord
  "Given unrolled ctor-style arguments creates a Kafka `ProducerRecord`."
  ([{:keys [:topic-name]} value]
   (ProducerRecord. ^String topic-name value))
  ([{:keys [:topic-name]} key value]
   (ProducerRecord. ^String topic-name key value))
  ([{:keys [:topic-name]} partition key value]
   (ProducerRecord. ^String topic-name
                    ^Integer (int partition)
                    key value))
  ([{:keys [:topic-name]} partition timestamp key value]
   (ProducerRecord. ^String topic-name
                    ^Integer (int partition)
                    ^Long (long timestamp)
                    key value))
  ([{:keys [:topic-name]} partition timestamp key value headers]
   (ProducerRecord. ^String topic-name
                    ^Integer (int partition)
                    ^Long (long timestamp)
                    key value
                    ^Headers headers)))

(defn map->ProducerRecord
  "Given a `::producer-record` build an equivalent `ProducerRecord`.

  Inverts `(datafy ^ProducerRecord r)`."
  [{:keys [:topic-name
           :key
           :value
           :headers
           :partition
           :timestamp] :as m}]
  (->ProducerRecord partition timestamp key value headers))

(defn->data ProducerRecord->data [^ProducerRecord pr]
  {:topic-name (.topic pr)
   :key (.key pr)
   :value (.value pr)
   :headers (.headers pr)
   :partition (.partition pr)
   :timestamp (.timestamp pr)})

(comment
  )

;;; Record metadata

(defn ->RecordMetadata
  "Given unrolled ctor-style arguments, create a Kafka `RecordMetadata`.

  Note that as of KIP-31, Kafka actually only stores offsets relative
  to a message batch on the wire or on disk. In order to maintain the
  previous abstraction that there's a \"offset\" field which is
  absolute, an additional arity is provided which lets the user
  construct a record with a base offset and a relative offset of 0 so
  that the metadata's apparent offset is predictable.

  Note that as the checksum is deprecated, by default it is not
  required. The third arity allows a user to provide a checksum. This
  arity may be removed in the future pending further breaking changes
  to the Kafka APIs."
  ([{:keys [:topic-name] :as t} partition offset timestamp key-size value-size]
   (RecordMetadata. (->TopicPartition t partition)
                    offset 0 ;; Force absolute offset
                    timestamp
                    nil ;; No checksum, it's deprecated
                    (int key-size)
                    (int value-size)))
  ([{:keys [:topic-name] :as t} partition base-offset relative-offset timestamp
    key-size value-size]
   (RecordMetadata. (->TopicPartition t partition)
                    base-offset
                    relative-offset ;; Full offset control
                    timestamp
                    nil ;; No checksum, it's depreciated
                    (int key-size)
                    (int value-size)))
  ([{:keys [:topic-name] :as t} partition base-offset relative-offset timestamp checksum
    key-size value-size]
   (RecordMetadata. (->TopicPartition t partition)
                    base-offset
                    relative-offset ;; Full offset control
                    timestamp
                    checksum ;; Have fun I guess
                    (int key-size)
                    (int value-size))))

(defn map->RecordMetadata
  "Given a `::record-metdata`, build an equivalent `RecordMetadata`.

  Inverts `(datafy ^RecordMetadata rm)`."
  [{:keys [:partition
           :timestamp
           :offset
           :serialized-key-size
           :serialized-value-size] :as m}]
  (->RecordMetadata m partition offset timestamp
                    serialized-key-size serialized-value-size))

(defn->data RecordMetadata->data [^RecordMetadata rm]
  {:topic-name (.topic rm)
   :partition (.partition rm)
   :timestamp (.timestamp rm)

   ;; As of Kafka 0.11.0 the checksum is deprecated. It is no longer
   ;; part of Kafka wire protocol, while the brokers may use
   ;; checksuming at reset to ensure message integrity.

   ;; :jackdaw.sent-record/checksum (.checksum rm)
   :offset (.offset rm)
   :serialized-key-size (.serializedKeySize rm)
   :serialized-value-size (.serializedValueSize rm)})

(comment
  (->RecordMetadata {:topic-name "foo"} 1 100 1 5 10)
  (RecordMetadata->data *1)
  (map->RecordMetadata *1)
  ;; On 1.10+
  (datafy *1))
