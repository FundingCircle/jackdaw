(ns kafka.zk
  (:require [clojure.string :as str])
  (:import kafka.utils.ZkUtils))

(def zk-connect "zookeeper.connect")
(def zk-session-timeout "zookeeper.session.timeout.ms")
(def zk-connection-timeout "zookeeper.connection.timeout.ms")

(defn- int-get
  [config key default]
  (try
    (Integer/parseInt (get config key default))
    (catch Exception e
      (let [msg (format "Invalid config value '%s' for key '%s'"
                        (get config key default)
                        key)
            context {:config config
                     :key key
                     :default default}]
        (throw (ex-info msg context e))))))

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
