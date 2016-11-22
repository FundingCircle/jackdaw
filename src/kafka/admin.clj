(ns kafka.admin
  (:require
   [clojurewerkz.propertied.properties :as p])
  (:import
   (kafka.admin AdminUtils)))

(defn create-topic!
  "Create `topic` as specified in the topic spec"
  [zk-utils topic partitions replication-factor config]
  (AdminUtils/createTopic zk-utils topic
                          (int partitions)
                          (int replication-factor)
                          (p/map->properties config)
                          nil))

(defn topic-exists?
  "Returns true if `topic` exists"
  [zk-utils topic]
  (AdminUtils/topicExists zk-utils topic))

(defn delete-topic!
  "Delete `topic`"
  [zk-utils topic]
  (AdminUtils/deleteTopic zk-utils topic))
