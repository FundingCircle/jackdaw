(ns kafka.test.kafka
  (:require
   [clojure.java.io :as io]
   [kafka.test.config :as config]
   [kafka.test.fs :as fs])
  (:import
   (kafka.server KafkaConfig KafkaServerStartable)))

(defn start!
  "Starts a kakfa broker.

   Returns a map containing the broker instance"
  [{:keys [config]}]
  (let [broker (-> config
                   (config/properties)
                   (KafkaConfig.)
                   (KafkaServerStartable.))]
    (.startup broker)
    {:broker broker}))


(defn stop!
  "Stops a kafka broker.

   Shuts down the broker and deletes its log files"
  [{:keys [broker config log-dirs]}]
  (when broker
    (try
      (.shutdown broker)
      {:broker nil}
      (finally
        (fs/try-delete! (io/file log-dirs))))))
