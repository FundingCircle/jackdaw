;; jackdaw.data.consumer
;;
;; jackdaw.data extensions for the consumer API types

(in-ns 'jackdaw.data)

(import '[org.apache.kafka.clients.consumer
          ConsumerRecord OffsetAndTimestamp]
        'org.apache.kafka.common.header.Headers
        'org.apache.kafka.common.record.TimestampType)

(set! *warn-on-reflection* true)

(defn ->ConsumerRecord
  "Given unrolled ctor-style arguments create a Kafka `ConsumerRecord`.

  Convenient for testing the consumer API and its helpers."
  ^ConsumerRecord [{:keys [:topic-name]} partition offset ts ts-type
   key-size value-size key value ^Headers headers]
  (ConsumerRecord. topic-name
                   (int partition)
                   (long offset)
                   (long ts)
                   (if (keyword? ts-type)
                     (->TimestampType ts-type)
                     ^TimestampType ts-type)
                   nil ;; Deprecated checksum
                   (int key-size)
                   (int value-size)
                   key value
                   headers))

(defn map->ConsumerRecord
  "Given a `::consumer-record`, build an equivalent `ConsumerRecord`.

  Inverts `(datafy ^ConsumerRecord cr)`."
  [{:keys [:key
           :value
           :headers
           :partition
           :timestamp
           :timestamp-type
           :offset
           :serialized-key-size
           :serialized-value-size]
    :as m}]
  (->ConsumerRecord m partition offset timestamp
                    (->TimestampType timestamp-type)
                    serialized-key-size serialized-value-size
                    key value headers))

(defn->data ConsumerRecord->data [^ConsumerRecord r]
  {:topic-name (.topic r)
   :key (.key r)
   :value (.value r)
   :headers (.headers r)
   :partition (.partition r)
   :timestamp (.timestamp r)
   ;; Deprecated field
   ;; :checksum (.checksum r)
   :timestamp-type (TimestampType->data (.timestampType r))
   :offset (.offset r)
   :serialized-key-size (.serializedKeySize r)
   :serialized-value-size (.serializedValueSize r)})

(comment
  (->ConsumerRecord {:topic-name "foo"} 1 100 1 :jackdaw.timestamp/create
                    5 10 "fooo" "barrrrrrrr" nil)
  (ConsumerRecord->data *1)
  (map->ConsumerRecord *1)
  ;; on 1.10+
  (datafy *1))

;;; OffsetAndTimestamp tuples

(defn ->OffsetAndTimestamp
  ^OffsetAndTimestamp [{:keys [offset timestamp]}]
  (OffsetAndTimestamp. offset (long timestamp)))

(defn->data OffsetAndTimestamp->data [^OffsetAndTimestamp ots]
  {:offset (.offset ots)
   :timestamp (.timestamp ots)})

(defn map->OffsetAndTimestamp [m]
  (->OffsetAndTimestamp m))

(defn as-OffsetAndTimestamp
  [ot]
  (cond (instance? OffsetAndTimestamp ot)
        ot

        (map? ot)
        (if (= OffsetAndTimestamp (:clojure.datafy/class (meta ot)))
          (:clojure.datafy/obj (meta ot))
          (map->OffsetAndTimestamp ot))))
