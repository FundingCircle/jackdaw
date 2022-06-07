(ns jackdaw.test.journal
  (:require
    [clojure.set :refer [subset?]]
    [clojure.tools.logging :as log]
    [manifold.stream :as s]
    [manifold.deferred :as d]))

(set! *warn-on-reflection* true)

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
                            (deliver p {:result :found
                                        :info result})))]
    (add-watch journal id (fn [_k _r _old new]
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
  "Journals the `record` in the appropriate place in the supplied test
   machine's `:journal`"
  [machine record]
  (let [journal (:journal machine)]
    (if-let [err (agent-error journal)]
      (throw err)
      (send journal journal-read [:topics (:topic record)] [record]))))

(defn journaller
  "Returns an asynchronous process that reads all messages produced by
   the supplied `machine`'s `:consumer` and records them in the journal"
  [machine stop?]
  (when-not (:journal machine)
    (log/error machine "no journal available")
    (throw (ex-info "no journal available: " {})))

  (when-not (get-in machine [:consumer :messages])
    (log/error machine "no message stream to journal")
    (throw (ex-info "no message stream to journal:" machine)))

  (let [{:keys [messages]} (:consumer machine)]
    (d/loop [record (s/take! messages)]
      (d/chain record
               (fn [record]
                 (when-not @stop?
                   (when record
                     (journal-result machine record)
                     (d/recur (s/take! messages)))))))))

(defn with-journal
  "Enriches the supplied `machine` with a journaller that will write to
   the supplied `journal`."
  [machine journal]
  (let [machine' (assoc machine :journal journal)
        stop? (atom false)
        jloop (journaller machine' stop?)]
    (assoc machine'
           :journal journal
           :jloop jloop
           :exit-hooks (concat
                        (:exit-hooks machine)
                        [#(do
                            (log/debug "stop accepting journal messages")
                            (reset! stop? true)
                            (log/debug "wait for journaller to finish")
                            @jloop)]))))

(defn raw-messages
  [journal topic-name]
  (sort-by :offset (get-in journal [:topics topic-name])))

(defn messages
  [journal topic-name]
  (->> (raw-messages journal topic-name)
       (map :value)))

(defn messages-by-kv-fn
  [journal topic-name ks pred]
  (->> (messages journal topic-name)
       (filter (fn [m]
                 (pred (get-in m ks))))))

(defn messages-by-kv
  [journal topic-name ks value]
  (messages-by-kv-fn journal topic-name ks #(= value %)))

(defn by-key
  "Returns the first message in the topic where attribute 'ks' is equal to 'value'. Can be
  combined with the :watch command to assert that a message has been published:

  [:watch (j/by-key :result-topic [:object :color] \"red\")]"
  [topic-name ks value]
  (fn [journal]
    (first (messages-by-kv journal topic-name ks value))))

(defn by-keys
  "Returns all of the messages in the topic where attribute 'ks' is equal to one of the values.
   Can be combined with the :watch command to assert that messages have been published:

  [:watch (j/by-key :result-topic [:object :color] #{\"red\" \"green\" \"blue\"})]"
  [topic-name ks values]
  (fn [journal]
    (messages-by-kv-fn journal topic-name ks (set values))))

(defn by-id
  "Returns all of the messages in the topic with an id of `value`. Can be combined with the
  :watch command to assert that a message with the supplied id has been published:

  [:watch (j/by-id :result-topic 123)]"
  [topic-name value]
  (by-key topic-name [:id] value))

(defn all-keys-present
  "Returns true if all the passed ids can be found in the topic by key ks"
  [topic-name ks ids]
  (fn [journal]
    (subset? (set ids)
             (->> (messages journal topic-name)
                  (map #(get-in % ks))
                  set))))
