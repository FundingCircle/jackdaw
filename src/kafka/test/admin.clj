(ns kafka.test.admin
  (:require
   [kafka.test.zk :as zk]
   [kafka.test.config :as config])
  (:import
   [kafka.admin AdminUtils]
   [kafka.utils]))

(defn create!
  "Create `topic` as specified in the topic spec"
  [zk-utils
   {:keys [topic partitions replication-factor config]}]
  (AdminUtils/createTopic zk-utils topic
                          (int partitions)
                          (int replication-factor)
                          (config/props config)
                          nil))

(defn exists?
  "Returns true if `topic` exists"
  [zk-utils topic]
  (AdminUtils/topicExists zk-utils topic))

(defn delete!
  "Delete `topic`"
  [zk-utils topic]
  (AdminUtils/deleteTopic zk-utils topic))
