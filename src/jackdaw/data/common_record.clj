(in-ns 'jackdaw.data)

(import 'org.apache.kafka.common.record.TimestampType)

(set! *warn-on-reflection* true)

;;; Timestamp types

(def +no-type-timestamp+
  "A timestamp type associated with... not having a timestamp type."
  TimestampType/NO_TIMESTAMP_TYPE)

(def +create-type-timestamp+
  "A timestamp type associated with the timestamp being from the
  record's creation. That is, the record timestamp was user supplied."
  TimestampType/CREATE_TIME)

(def +log-append-type-timestamp+
  "A timestamp type associated with the timestamp having been generated
  by Kafka when the record was produced, not having been specified by
  the user when the record was created."
  TimestampType/LOG_APPEND_TIME)

(defn ->TimestampType
  "Given a keyword being a datafied Kafka `TimestampType`, return the
  equivalent `TimestampType` instance."
  ^TimestampType [kw]
  (case kw
    :timestamp-type/create TimestampType/CREATE_TIME
    :timestamp-type/log-append TimestampType/LOG_APPEND_TIME
    TimestampType/NO_TIMESTAMP_TYPE))

(defn->data TimestampType->data [^TimestampType tt]
  (cond
    (= tt TimestampType/NO_TIMESTAMP_TYPE) nil
    (= tt TimestampType/CREATE_TIME) :timestamp-type/create
    (= tt TimestampType/LOG_APPEND_TIME) :timestamp-type/log-append))

(comment
  (->TimestampType
   (TimestampType->data +no-type-timestamp+))

  (->TimestampType
   (TimestampType->data +create-type-timestamp+))

  (->TimestampType
   (TimestampType->data +log-append-type-timestamp+)))
