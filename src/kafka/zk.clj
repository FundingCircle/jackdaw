(ns kafka.test.zk
  (:require
   [com.stuartsierra.component :as component]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [kafka.test.fs :as fs]
   [manifold.deferred :as d])
  (:import
   (java.net InetSocketAddress)
   (kafka.utils ZkUtils)
   (org.apache.zookeeper.server ZooKeeperServer ServerCnxnFactory)
   (org.apache.zookeeper KeeperException$NoNodeException)
   (org.I0Itec.zkclient ZkClient ZkConnection)))

(def zk-connect "zookeeper.connect")
(def zk-session-timeout "zookeeper.session.timeout.ms")
(def zk-connection-timeout "zookeeper.connection.timeout.ms")

(defn- ensure-connect-string
  [config]
  (when-not (get config zk-connect)
    (throw (ex-info "Zookeeper connection info not set"
                    config))))

(defn port
  "Parse the zookeeper port out of a kafka server config"
  [connect-string]
  (-> connect-string
      (str/split #":")
      (nth 1)
      read-string))

(defn client
  [config]
  (ensure-connect-string config)

  (let [connect         (get config zk-connect)
        connect-timeout (int-get config zk-connection-timeout "1000")
        session-timeout (int-get config zk-session-timeout "5000")]
    (ZkUtils/createZkClient connect connect-timeout session-timeout)))

(defn utils
  [zk-client]
  (ZkUtils/apply zk-client false))
