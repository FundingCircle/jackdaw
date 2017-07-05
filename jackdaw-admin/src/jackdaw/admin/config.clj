(ns jackdaw.admin.config
  (:require [environ.core :refer [env]]))

(def num-retries (env :num-retries 3))

(def wait-ms 1000)

(def common
   {:connect-string (env :zookeeper-address)
   :bootstrap-servers (env :bootstrap-servers)
   :default-partitions (env :partitions 1)
   :default-replication (env :replication 1)})
