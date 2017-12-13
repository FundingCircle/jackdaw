(ns jackdaw.test.kafka
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [jackdaw.test.fs :as fs])
  (:import
   (kafka.server BrokerMetadata
                 KafkaConfig
                 BrokerMetadataCheckpoint
                 KafkaServerStartable)))

(defn start!
  "Starts a kakfa broker.

   Returns a map containing the broker instance itself and a latch
   that waits until the broker is shutdown"
  [{:keys [config]}]
  (let [props           (java.util.Properties.)
        checkpoint-file (io/file (get config "log.dirs") "meta.properties")
        broker-id       (Integer/parseInt (get config "broker.id"))]
    ;; Create the configured log directory
    (io/make-parents checkpoint-file)
    
    ;; Kafka expects a meta.properties file to exist in the configured
    ;; log directory. The meta.properties structure is a serialized
    ;; BrokerMetadataCheckpoint object, so just instantiate one and
    ;; use its own writing machinery.
    (. (BrokerMetadataCheckpoint. checkpoint-file)
       (write (BrokerMetadata. broker-id)))

    ;; Generate the KafkaConfig (java.util.Properties) map that we
    ;; need to start the broker
    (.putAll props config)
    (let [broker (-> props
                     (KafkaConfig.)
                     (KafkaServerStartable.))]
      (.startup broker)
      {:broker broker})))

(defn stop!
  "Stops a kafka broker.

   Shuts down the broker, releases the latch, and deletes log files"
  [{:keys [broker config log-dirs]}]
  (when broker
    (try
      (.shutdown broker)
      (.awaitShutdown broker)
      {:broker nil}
      (finally
        (fs/try-delete! (io/file log-dirs))))))
