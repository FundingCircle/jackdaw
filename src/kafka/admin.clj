(ns kafka.admin
  (:require
   [kafka.config :as config])
  (:import
   [kafka.admin AdminUtils]
   [kafka.utils]))

(defn create-topic
  "Create `topic` as specified in the topic spec"
  [zk-utils topic partitions replication-factor config]
  (AdminUtils/createTopic zk-utils topic
                          (int partitions)
                          (int replication-factor)
                          (config/props config)
                          nil))

(defn topic-exists?
  "Returns true if `topic` exists"
  [zk-utils topic]
  (AdminUtils/topicExists zk-utils topic))

(defn delete-topic
  "Delete `topic`"
  [zk-utils topic]
  (AdminUtils/deleteTopic zk-utils topic))
