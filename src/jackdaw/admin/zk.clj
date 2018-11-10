(ns jackdaw.admin.zk
  (:import [kafka.utils ZkUtils]
           [scala.collection JavaConversions]))

(def default_zk_session_timeout_ms 10000)

(def default_zk_connection_timeout_ms 8000)

(defn connect-string
  "The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
  Example: `127.0.0.1:2181`.
  You can use this to e.g. tell Kafka brokers how to connect to this instance. "
  [zk]
  (.getConnectString zk))

;; https://github.com/kafka-dev/kafka/blob/master/core/src/main/scala/kafka/utils/ZkUtils.scala

(defn zk-utils
  "Returns `kafka.utils.ZkUtils` instance exposing utilities to inspect and manipulate Zookeeper."
  [connect-str]
  (ZkUtils/apply (ZkUtils/createZkClient connect-str
                                         default_zk_session_timeout_ms
                                         default_zk_connection_timeout_ms)
                 false))

(defn get-cluster
  "Returns a map ID to broker description for all the available Kafka brokers."
  [^ZkUtils zk-utils]
  (as-> zk-utils %
    ;; Get the Cluster - which is just a dumb shim class
    (.getCluster %)
    ;; Convert the Cluster to a normal map.
    ;;
    ;; Brokers are integer ID'd, but the actual map they're in is a private
    ;; class member. So while we know the NUMBER of brokers, we can't actually
    ;; get them because their IDs are NOT required to be sequential. Eg. if
    ;; there should be four brokers, but two are down, then the cluster size
    ;; will be two but the IDs could be 1 and 4.
    ;;
    ;; Consequently, we scan over ALL THE POSSIBLE IDS until we have the broker
    ;; count we expect. Or we hit 100, because that's silly.
    (reduce (fn [acc i]
              (if (= (count acc) (.size %))
                (reduced acc)
                (if-let [^kafka.cluster.Broker broker (.get ^scala.Some (.getBroker ^kafka.cluster.Cluster % i))]
                  (assoc acc i broker)
                  acc)))
            {} (range 100))))
