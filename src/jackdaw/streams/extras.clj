(ns jackdaw.streams.extras
  "FIXME"
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.tools.logging :as log]
            [jackdaw.streams :as js]
            [clojure.spec.alpha :as s])
  (:import org.apache.kafka.common.TopicPartition
           org.apache.kafka.streams.processor.StateRestoreListener
           [java.time Instant Duration]))

(set! *warn-on-reflection* true)

(defn map-validating!
  [builder topic topic-spec {:keys [line file]}]
  (js/map-values builder
                 (fn [val]
                   (if-not (s/valid? topic-spec val)
                     (let [msg "Failed to validate outbound record!"
                           data (merge {::topic (:topic.metadata/name topic)
                                        ::line  line
                                        ::file  file}
                                       (s/explain-data topic-spec val))]
                       (log/fatal msg (pr-str data))
                       (throw (ex-info msg data)))
                     val))))

(defn with-file [form-meta]
  (update form-meta :file #(or %1 *file*)))

(defn logging-state-restore-listener
  "Returns a new Kafka `StateRestoreListener` instance which logs when
  batches are restored, and how long it takes to restore all the
  batches for a given partition."
  ^StateRestoreListener []
  (let [restore-tracker (atom {})]
    (reify StateRestoreListener
      (^void onRestoreStart [_
                             ^TopicPartition topicPartition,
                             ^String storeName
                             ^long startingOffset
                             ^long endingOffset]
       (swap! restore-tracker assoc storeName (Instant/now))
       (log/warnf "Restoring state store (%s.%d) over offsets %s...%s"
                  (.topic topicPartition) (.partition topicPartition)
                  startingOffset endingOffset))

      (^void onBatchRestored [_
                              ^TopicPartition topicPartition
                              ^String storeName
                              ^long batchEndOffset
                              ^long numRestored]
       (log/warnf "Restored a batch from (%s.%d)"
                  (.topic topicPartition) (.partition topicPartition)))
      (^void onRestoreEnd [_
                           ^TopicPartition topicPartition
                           ^String storeName
                           ^long totalRestored]
       (let [start-date  (get @restore-tracker storeName)
             elapsed-sec (.getSeconds (Duration/between start-date
                                                        (Instant/now)))]
         (log/warnf "Finished restoring state store (%s.%d) elapsed %s"
                    (.topic topicPartition) (.partition topicPartition)
                    elapsed-sec))))))

(defmacro to!
  "Wraps `#'jackdaw.streams/to!`, providing validation of records
  against the spec of the to! topic."
  ([builder topic topic-spec]
   `(let [t# ~topic]
      (-> ~builder
          (map-validating! t# ~topic-spec ~(with-file (meta &form)))
          (js/to t#))))
  ([builder partition-fn topic topic-spec]
   `(let [t# ~topic]
      (-> ~builder
          (map-validating! t# ~topic-spec ~(with-file (meta &form)))
          (js/to ~partition-fn t#)))))

(defmacro through
  "Wraps `#'jackdaw.streams/through`, providing validation of records
  against the spec of the through topic."
  ([builder topic topic-spec]
   `(let [t# ~topic]
      (-> ~builder
          (map-validating! t# ~topic-spec ~(with-file (meta &form)))
          (js/through t#))))
  ([builder partition-fn topic topic-spec]
   `(let [t# ~topic]
      (-> ~builder
          (map-validating! t# ~topic-spec ~(with-file (meta &form)))
          (js/through ~partition-fn t#)))))
