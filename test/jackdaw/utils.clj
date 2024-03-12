(ns jackdaw.utils)

(defn bootstrap-servers
  []
  (str (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS") "localhost")
       ":"
       (or (System/getenv "KAFKA_PORT") "9092")))

(defn zookeeper-address
  []
  (str (or (System/getenv "ZOOKEEPER_HOST") "localhost")
       ":"
       (or (System/getenv "ZOOKEEPER_PORT") "2181")))

(defn schema-registry-address
  []
  (str (or (System/getenv "SCHEMA_REGISTRY_HOST") "localhost")
       ":"
       (or (System/getenv "SCHEMA_REGISTRY_PORT") "8081")))

(defn kafka-rest-proxy-address
  []
  (str (or (System/getenv "KAFKA_REST_PROXY_HOST") "localhost")
       ":"
       (or (System/getenv "KAFKA_REST_PROXY_PORT") "8082")))