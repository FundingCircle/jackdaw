(ns jackdaw.streams.extras
  "FIXME"
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.tools.logging :as log]
            [jackdaw.streams :as js]
            [jackdaw.streams.lambdas :as lambdas]
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

(defn seen!?
  "Returns true if `dedupe-key` is already present in state-store.
  Otherwise returns false, and adds the key to the state-store."
  [state-store dedupe-key]
  (if (.get state-store dedupe-key)
    true
    (do
     (.put state-store dedupe-key (System/currentTimeMillis))
     false)))

(defn dedupe-by
  "Filters out any messages in the stream already seen. Seen keys are
  held in a state store. Key to use for de-duping is accessed via
  `unique-key-fn`, which is passed a vector of `[k v]`
  If `dedupe-notifier-fn` is set, it is called with `[k v]` when a
  message is dropped from the stream. Typically this would be used to
  pass in logging for dropped messages."
  [kstream dedupe-store-name unique-key-fn dedupe-notifier-fn]
  (js/transform kstream
                (lambdas/transformer-with-ctx
                 (lambdas/with-stores
                   [dedupe-store-name]
                   (fn [_ stores k v]
                     (if (seen!? (get stores dedupe-store-name) (unique-key-fn [k v]))
                       (do
                        (when dedupe-notifier-fn
                          (dedupe-notifier-fn [k v]))
                        nil)
                       (lambdas/key-value [k v])))))
                [dedupe-store-name]))

(defn dedupe-by-key
  "Filters out any messages in the stream already seen. Seen keys are
  held in a state store. Uses the stream's key for deduping."
  ([kstream dedupe-store-name]
   (dedupe-by-key kstream dedupe-store-name nil))
  ([kstream dedupe-store-name dedupe-notifier-fn]
   (dedupe-by kstream dedupe-store-name (fn [[k _]] k) dedupe-notifier-fn)))

(defn dedupe-by-field
  "Filters out any messages in the stream already seen. Seen keys are
  held in a state store. Uses the result of applying `key-fn` to the
  stream's value for deduping."
  ([kstream dedupe-store-name key-fn]
   (dedupe-by-field kstream dedupe-store-name key-fn nil))
  ([kstream dedupe-store-name key-fn dedupe-notifier-fn]
   (dedupe-by kstream dedupe-store-name (fn [[_ v]] (key-fn v)) dedupe-notifier-fn)))
