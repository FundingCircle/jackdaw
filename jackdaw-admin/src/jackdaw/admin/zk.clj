(ns jackdaw.admin.zk
  (:import [kafka.utils ZkUtils]
           [scala.collection JavaConversions]))

(def default_zk_session_timeout_ms 10000)

(def default_zk_connection_timeout_ms 8000)

(defn zk-utils
  "Returns `kafka.utils.ZkUtils` instance exposing utilities to inspect and manipulate Zookeeper."
  [connect-str]
  (ZkUtils/apply (ZkUtils/createZkClient connect-str default_zk_session_timeout_ms default_zk_connection_timeout_ms) false))

(defn connect-string
  "The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
  Example: `127.0.0.1:2181`.
  You can use this to e.g. tell Kafka brokers how to connect to this instance. "
  [zk]
  (.getConnectString zk))
