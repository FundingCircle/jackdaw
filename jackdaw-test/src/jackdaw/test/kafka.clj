(ns jackdaw.test.kafka
  (:require
   [clojure.java.io :as io]
   [clojurewerkz.propertied.properties :as p]
   [jackdaw.test.fs :as fs])
  (:import
   (kafka.server KafkaConfig
                 KafkaServerStartable)))

(defn start!
  "Starts a kakfa broker.

   Returns a map containing the broker instance itself and a latch
   that waits until the broker is shutdown"
  [{:keys [config]}]
  (let [broker (-> config
                   (p/map->properties)
                   (KafkaConfig.)
                   (KafkaServerStartable.))]
    (.startup broker)
    {:broker broker}))

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
