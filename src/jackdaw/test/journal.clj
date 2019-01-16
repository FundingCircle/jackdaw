(ns jackdaw.test.journal
  ""
  (:require
   [clojure.tools.logging :as log]
   [clojure.core.async :as async]))

;; Journal
;;
;; The journal represents the test output. It captures the output of all
;; messages arriving on the test-machine' `:consumer` channel.

(defn watch-for
  "Returns the first true application of the journal to the specified condition `condition?`"
  [machine condition? timeout info]
  (let [journal (:journal machine)
        p (promise)
        id (java.util.UUID/randomUUID)
        check-condition (fn [new]
                          (when-let [result (condition? new)]
                            (remove-watch journal id)
                            (if (boolean? result)
                              (deliver p {:result :found})
                              (deliver p result))))]

    (add-watch journal id (fn [k r old new]
                            (check-condition new)))
    ;; don't rely on watcher to 'check-condition'
    ;; in case journal is already in a final, good state
    (check-condition @journal)
    (deref p timeout {:error :timeout :info info})))

(defn journal-read
  "Append `record` into the `journal` under `journal-key`"
  [journal journal-key v]
  (let [result (update-in journal journal-key concat v)]
    result))

(defn reverse-lookup
  "Given a map of `topic-metadata`, find the name of the entry whose
   `:topic-name` matches the supplied topic.

   The name supplied in the key positions in the supplied map do not necessarily
   match the topic name. This allows users to provide a 'logical' name that may stay
   constant if the user wishes over version changes.

   This function is used when populating the journal to ensure that the topic
   identifiers given by the user are used when populating the journal (instead of
   the real topic names)."
  [topic-metadata topic]
  (let [m (reduce-kv (fn [m k v]
                       (assoc m (:topic-name v) k))
                     {}
                     topic-metadata)]
    (get m topic)))

(defn journal-result
  [machine record]
  "Journals the `record` in the appropriate place in the supplied test
   machine's `:journal`"
  (let [journal (:journal machine)]
    (if-let [err (agent-error journal)]
      (throw err)
      (do
        (send journal journal-read [:topics (:topic record)] [record])))))

(defn journaller
  "Returns an asynchronous process that reads all messages produced by
   the supplied `machine`'s `:consumer` and records them in the journal"
  [machine]
  (when-not (:journal machine)
    (log/error machine "no journal available")
    (throw (ex-info "no journal available: " {})))

  (when-not (get-in machine [:consumer :messages])
    (log/error machine "no message stream to journal")
    (throw (ex-info "no message stream to journal:" machine)))

  (let [{:keys [messages]} (:consumer machine)]
    (async/go-loop [record (async/<! messages)]
      (when record
        (journal-result machine record)
        (recur (async/<! messages))))))

(defn with-journal
  "Enriches the supplied `machine` with a journaller that will write to
   the supplied `journal`."
  [machine journal]
  (let [machine' (assoc machine :journal journal)
        jloop (journaller machine')]
    (assoc machine'
           :journal journal
           :jloop jloop
           :exit-hooks (concat
                        (:exit-hooks machine)
                        [#(do (log/debug "closing jloop")
                              (async/close! jloop))]))))
