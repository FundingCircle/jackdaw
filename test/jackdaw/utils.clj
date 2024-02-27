(ns jackdaw.utils)

(defn bootstrap-servers
  []
  (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS") "localhost"))

(defn zookeeper-host
  []
  (or (System/getenv "ZOOKEEPER_HOST") "localhost"))

(defn schema-registry-host
  []
  (or (System/getenv "SCHEMA_REGISTRY_HOST") "localhost"))

(defn kafka-rest-proxy-host
  []
  (or (System/getenv "KAFKA_REST_PROXY_HOST") "localhost"))