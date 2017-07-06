(ns jackdaw.admin.config
  (:require [environ.core :refer [env]]))

(def common
  {"zookeeper.connect"          (env :zookeeper-address)
   "bootstrap.servers"          (env :bootstrap-servers)
   "num.partitions"             (env :partitions 1)
   "default.replication.factor" (env :replication 1)})
