(ns jackdaw.client.log
  "Extras for `jackdaw.client` for treating topics as seqs of records.

  Pretty handy for testing."
  (:require [clojure.tools.logging :as log]
            [jackdaw.client :as jc])
  (:import org.apache.kafka.clients.consumer.Consumer))

(defn log
  "Given a consumer, returns a lazy sequence of datafied consumer records.

  If fuse-fn was provided, stops after fuse-fn returns false."
  ([^Consumer consumer polling-interval-ms]
   (log consumer polling-interval-ms (constantly true)))
  ([^Consumer consumer polling-interval-ms fuse-fn]
   (let [r (jc/poll consumer polling-interval-ms)]
     (if (fuse-fn r)
       (do (log/debugf "Got %d records" (count r))
           (lazy-cat r (log consumer polling-interval-ms fuse-fn)))
       r))))

(defn log-until
  "Given a consumer, a number of MS at which to poll and a duration,
  returns a lazy sequence of datafied consumer records consumed during
  that period.

  Stops when current time > end-at."
  [^Consumer consumer polling-interval-ms end-at-ms]
  (seq (log consumer
            polling-interval-ms
            (fn [_]
              (< (System/currentTimeMillis)
                 end-at-ms)))))

(defn log-until-inactivity
  "Given a consumer, returns a lazy sequence of datafied consumer records.

  Stops when no messages are returned from poll."
  [^Consumer consumer polling-interval-ms]
  (log consumer polling-interval-ms seq))

(defn wait-for-inactivity
  "Waits until no messages have been produced on the topic in the given duration."
  [config topic inactivity-ms]
  (with-open [^Consumer consumer (jc/subscribed-consumer config topic)]
    (log/infof "Skipped %d messages"
               (count (log-until-inactivity consumer inactivity-ms)))))

;; FIXME (reid.mckenzie 2018-11-17):
;;
;;   This is the worst possible implementation. Offsets are
;;   monotonic. Seek to beginning, get the current offsets, seek to
;;   the end, get the current offsets, return the sum of the
;;   differences.
;;
;;   Consuming all the messages is insane.
(defn count-messages
  "Consumes all of the messages on a topic to determine the current count"
  [config topic]
  (with-open [^Consumer consumer (-> (jc/subscribed-consumer config topic)
                                     (jc/seek-to-beginning-eager))]
    (count (log-until-inactivity consumer 2000))))
