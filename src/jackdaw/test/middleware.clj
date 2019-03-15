(ns jackdaw.test.middleware
  "A test machine executor can be built by composing execution wrappers
   defined in here")

(defn with-status
  [f]
  (fn [machine cmd]
    (let [result (f machine cmd)]
      (assoc result
             :status (if (contains? result :error)
                       :error
                       :ok)))))

(defn with-timing
  "Timing middleware. Adds timing information to the response map"
  [f]
  (fn [machine cmd]
    (let [start (System/nanoTime)
          result (f machine cmd)
          end (System/nanoTime)
          millis (fn [t]
                   (/ (double t) 1000000.0))]
      (assoc result
             :started-at (millis start)
             :finished-at (millis end)
             :duration (- (millis end) (millis start))))))

(defn with-journal-snapshots
  "Journal middleware. Adds journal snapshots before/after the operation
   to the response map"
  [f]
  (fn [machine cmd]
    (let [journal (:journal machine)
          before @journal
          result (f machine cmd)
          after @journal]
      (assoc result
             :journal-before before
             :journal-after after))))
