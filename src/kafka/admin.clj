(ns kafka.admin
  (:require
   [clojurewerkz.propertied.properties :as p])
  (:import
   (kafka.admin AdminUtils)))

(defn create-topic!
  "Create `topic` as specified in the topic spec"
  [zk-utils
   {:keys [topic.metadata/name
           topic.metadata/partitions
           topic.metadata/replication-factor]}
   config]
  (AdminUtils/createTopic zk-utils name
                          (int partitions)
                          (int replication-factor)
                          (p/map->properties config)
                          nil))

(defn topic-exists?
  "Returns true if `topic` exists"
  [zk-utils {:keys [topic.metadata/name]}]
  (AdminUtils/topicExists zk-utils name))

(defn delete-topic!
  "Delete `topic`"
  [zk-utils {:keys [topic.metadata/name]}]
  (AdminUtils/deleteTopic zk-utils name))
