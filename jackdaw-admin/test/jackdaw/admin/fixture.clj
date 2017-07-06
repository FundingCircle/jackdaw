(ns jackdaw.admin.fixture
  (:require [clojure.test :refer [join-fixtures use-fixtures]]
            [jackdaw.admin.config :as config]
            [jackdaw.test.fixtures :as fix]
            [jackdaw.test.fs :as fs]))

(def broker-conf
  {"broker.id"                    "1"
   "zookeeper.connect"            (:connect-string config/common)
   "port"                         "9092"
   "offsets.topic.num.partitions" "1"
   "auto.create.topics.enable"    "true"
   "delete.topic.enable"          "true"
   "controlled.shutdown.enable"   "true"
   "log.dirs"                     (fs/tmp-dir "kafka-log")})

(def zk-conf
  {"zookeeper.connect" (:connect-string config/common)})

(defn kafka
  ([]
   (kafka {}))
  ([{:keys [kafka-config zk-config]
     :or {kafka-config broker-conf
          zk-config zk-conf}}]
   (use-fixtures :once (join-fixtures [(fix/zookeeper zk-config)
                                       (fix/broker kafka-config)]))))
