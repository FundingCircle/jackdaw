(ns roll-dice.kafka
  (:require
   [jackdaw.client :as jc]
   [jackdaw.serdes :refer [string-serde edn-serde]]
   [taoensso.timbre :refer [error info]])
  (:import [org.apache.kafka.common.errors WakeupException]))

(def kafka-config {"bootstrap.servers" "localhost:9092"})

(defn producer-config [topic]
  (merge kafka-config
         {"acks" "all"
          "client.id" (str "producer-" (name topic))}))

(defn consumer-config [topic group-id]
  (merge kafka-config
         {"group.id" group-id
          "client.id" (str "consumer-" (name topic))
          "auto.offset.reset" "earliest"}))

(defn topic-config [topic]
  {:topic-name topic
   :key-serde (string-serde)
   :value-serde (edn-serde)})

(defn poll-and-loop!
  "Continuously fetches records every `poll-ms`, processes them and commits offset after each poll."
  [consumer processing-fn continue?]
  (let [poll-ms 5000]
    (loop []
      (if @continue?
        (let [records (jc/poll consumer poll-ms)]
          (when (seq records)
            (processing-fn records)
            (info "commit sync at offset" (-> records last :offset inc))
            (.commitSync consumer))
          (recur))))))

(defn stop-and-close-consumer!
  "Stops the consumer polling loop and closes the consumer."
  [consumer continue?]
  (reset! continue? false)
  (.close consumer)
  (info "Closed Kafka Consumer"))

(defn start-consumer!
  "Starts consumer loop to process events read from `topic`"
  [consumer processing-fn continue?]
  (try
    (poll-and-loop! consumer processing-fn continue?)
    (catch WakeupException e) ;; ignore for shutdown
    (finally
      (stop-and-close-consumer! consumer continue?))))

(defn add-shutdown-hook-consumer!
  "Registers a shutdown hook to exit the consumer cleanly"
  [consumer continue?]
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. (fn []
                               (info "Stopping Kafka Consumer...")
                               (reset! continue? false)
                               (.wakeup consumer))))) ; wakeup causes consumer to break out of polling

(defn process-messages!
  "Creates Kafka Consumer and shutdown hook, and starts the consumer"
  [topic group-id processing-fn]
  (let [topic-config    (topic-config topic)
        consumer-config (consumer-config topic group-id)
        continue?       (atom true)
        consumer        (jc/subscribed-consumer consumer-config [topic-config])]
      (add-shutdown-hook-consumer! consumer continue?)
      (start-consumer! consumer processing-fn continue?)))

(defn produce-message!
  "Creates a Kafka Producer and writes message to `topic` by calling `producer-fn`"
  [topic producer-fn]
  (let [topic-config      (topic-config topic)
        producer-config   (producer-config topic)]
    (with-open [producer (jc/producer producer-config topic-config)]
      (let [values (producer-fn)]
        @(jc/produce! producer topic-config values)))))
