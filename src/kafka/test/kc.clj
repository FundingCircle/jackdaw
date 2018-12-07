(ns kafka.test.kc
  (:import [io.confluent.kafka.schemaregistry.rest SchemaRegistryConfig SchemaRegistryRestApplication]
           [org.apache.kafka.common.utils SystemTime]
           [org.apache.kafka.connect.runtime Connect ConnectorFactory Herder Worker]
           [org.apache.kafka.connect.runtime.rest RestServer]
           [org.apache.kafka.connect.runtime.standalone StandaloneConfig StandaloneHerder]
           [org.apache.kafka.connect.storage FileOffsetBackingStore]))

(defn start!
  "Starts a kafka-connect standalone instance.

   Returns a map containing the kafka-connect instance itself and a latch
   that waits until the instance is shutdown"
  [{:keys [config]}]
  (let [tmp-dir (java.nio.file.Files/createTempDirectory
                  "embedded-kafka-connect-config-" (into-array java.nio.file.attribute.FileAttribute []))
        connector-factory (ConnectorFactory.)
        standalone-config (StandaloneConfig. (assoc config
                                                    "offset.storage.file.filename" (format "%s/worker-offsets" tmp-dir)))
        rest-server (RestServer. standalone-config)
        advertised-url (.advertisedUrl rest-server)
        worker-id (str (.getHost advertised-url) ":" (.getPort advertised-url))
        worker (Worker. worker-id (SystemTime.) connector-factory standalone-config (FileOffsetBackingStore.))
        herder (StandaloneHerder. worker)
        connect (Connect. herder rest-server)]

      (.start connect)

    (assoc config :kafka-connect connect)))

(defn stop!
  "Stops a kafka connect instance.

   Shuts down kafka-connect, releases the latch, and deletes log files"
  [{:keys [kafka-connect config]}]
  (when kafka-connect
    (.stop kafka-connect)
    {:kafka-connect nil}))
