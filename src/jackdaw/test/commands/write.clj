(ns jackdaw.test.commands.write
  (:require
   [clojure.core.async :as async]
   [clojure.tools.logging :as log]
   [jackdaw.client.partitioning :as partitioning]))

(defn default-partition-fn [topic-map k]
  (int (partitioning/default-partition topic-map k nil (:partition-count topic-map))))

(defn create-message [machine topic-map message opts]
  ;; By default the message will use the `:id` field as the key on kafka
  ;; and run the default partitioning function for the partition (which
  ;; works the same as the kafka one). This behaviour can be changed as follows:
  ;;  - If the topic map contains a `:key-fn`, use that function to extract
  ;;    the key from the message
  ;;  - If the topic map contains a `:partition-fn`, use that function to
  ;;    determine the partition to write to (should be an fn of airity 2
  ;;    as per `default-partition-fn`
  ;;  - The `:key-fn` and `:partition-fn` can also be passed separately in
  ;;    the options map `opts`.
  ;;  - Further, the `opts` map can contain an explciit `:key` and/or
  ;;    `:partition`, which if set will provide the values to use
  ;; If both specified, `opts` values will override values in the topic map.
  (let [key-fn (or (:key-fn opts)
                   (:key-fn topic-map)
                   :id)
        partition-fn (or (:partition-fn opts)
                         (:partition-fn topic-map)
                         default-partition-fn)
        k (if-let [explicit-key (:key opts)]
            explicit-key
            (key-fn message))
        _ (clojure.pprint/pprint k)
        _ (clojure.pprint/pprint topic-map)
        partn (if-let [explicit-partition (:partition opts)]
                explicit-partition
                (partition-fn topic-map k))
        timestamp (:timestamp opts (System/currentTimeMillis))]
    {:topic topic-map
     :key k
     :value message
     :partition partn
     :timestamp timestamp}))

(defn do-write
  ([machine topic-name message]
   (do-write machine topic-name message {}))
  ([machine topic-name message opts]
   (if-let [topic-map (get (:topics machine) topic-name)]
     (let [to-send (create-message machine topic-map message opts)
           messages (:messages (:producer machine))
           ack (promise)]
       (log/info "Sending" to-send "to topic" topic-name)
       (async/put! messages (assoc to-send :ack ack))
       (deref ack (:timeout opts 1000) {:error :timeout}))
     {:error :unknown-topic
      :topic topic-name
      :known-topics (keys (:topic-config machine))})))


(defn handle-write-cmd [machine cmd params]
  (apply do-write machine params))

(def command-map
  {:jackdaw.test.commands/write! handle-write-cmd})
