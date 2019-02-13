(ns jackdaw.test-config)

(defn test-config
  []
  (let [e (System/getenv)]
    {
     :broker            {:host (get e "BROKER_HOST" "localhost")
                         :port (get e "BROKER_PORT" 9092)}

     :kafka-rest        {:host (get e "KAFKA_REST_HOST" "localhost")
                         :port (get e "KAFKA_REST_PORT" 8082)}

     :schema-registry   {:host (get e "SCHEMA_REGISTRY_HOST" "localhost")
                         :port (get e "SCHEMA_REGISTRY_PORT" 8081)}

     :zookeeper         {:host (get e "ZOOKEEPER_HOST" "localhost")
                         :port (get e "ZOOKEEPER_PORT" 2181)}
     }))
