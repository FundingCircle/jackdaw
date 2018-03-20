(ns jackdaw.admin.config
  (:require [environ.core :refer [env]]))

(def common
  {"zookeeper.connect"          (env :zookeeper-address "localhost:2181")
   "bootstrap.servers"          (env :bootstrap-servers "localhost:9092")
   "num.partitions"             (env :partitions 1)
   "default.replication.factor" (env :replication 1)})
