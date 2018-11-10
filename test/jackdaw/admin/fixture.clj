(ns jackdaw.admin.fixture
  (:require [clojure.test :refer [join-fixtures use-fixtures]]
            [jackdaw.admin.config :as config]))

;; FIXME morgan.astra <2018-11-09 Fri>
;; These tests rely on the old deleted jackdaw.test namespace.
;; They haven't worked for a while, but we should write a new test maybe?

;; (def broker-conf
;;   {"broker.id"                    "1"
;;    "zookeeper.connect"            (get config/common "zookeeper.connect")
;;    "port"                         "9092"
;;    "offsets.topic.num.partitions" "1"
;;    "auto.create.topics.enable"    "true"
;;    "delete.topic.enable"          "true"
;;    "controlled.shutdown.enable"   "true"
;;    "log.dirs"                     (fs/tmp-dir "kafka-log")})

;; (def zk-conf
;;   {"zookeeper.connect" (get config/common "zookeeper.connect")})

;; (defn kafka
;;   ([]
;;    (kafka {}))
;;   ([{:keys [kafka-config zk-config]
;;      :or {kafka-config broker-conf
;;           zk-config zk-conf}}]
;;    (use-fixtures :once (join-fixtures [(fix/zookeeper zk-config)
;;                                        (fix/broker kafka-config)]))))
