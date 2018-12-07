(ns kafka.test.admin
  (:require
   [kafka.test.zk :as zk]
   [kafka.test.config :as config])
  (:import
   [kafka.admin AdminUtils]
   [kafka.utils]))

(defn create! [zk-utils
                    {:keys [topic partitions replication-factor config]}]
  (AdminUtils/createTopic zk-utils topic
                          (int partitions)
                          (int replication-factor)
                          (config/props config)
                          nil))

(defn exists? [zk-utils topic]
  (AdminUtils/topicExists zk-utils topic))

(defn delete! [zk-utils topic]
  (AdminUtils/deleteTopic zk-utils topic))
