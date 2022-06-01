(in-ns 'jackdaw.data)

(import '[org.apache.kafka.clients.admin
          Config ConfigEntry DescribeClusterResult NewTopic
          TopicDescription])

(set! *warn-on-reflection* true)

;;; ConfigEntry

(defn ->ConfigEntry
  "value can be a string else is a map where value is the :value key"
  ^ConfigEntry [^String k v]
  (if (string? v)
    (ConfigEntry. k v)
    (ConfigEntry. k (:value v))))

(defn->data ConfigEntry->data
  [^ConfigEntry e]
  {:name (.name e)
   :value (.value e)
   :default? (.isDefault e)
   :read-only? (.isReadOnly e)
   :sensitive? (.isSensitive e)})

;;; Config

(defn map->Config
  ^Config [m]
  (Config.
   (map (partial apply ->ConfigEntry) m)))

(defn->data Config->data
  [^Config c]
  (into {}
        (comp (map ConfigEntry->data)
              (map (fn [{:keys [name] :as e}]
                     [name e])))
        (.entries c)))

;;; TopicDescription

(defn->data TopicDescription->data
  [^TopicDescription td]
  {:is-internal? (.isInternal td)
   :partition-info (map datafy (.partitions td))})

;;; NewTopic

(defn map->NewTopic
  [{:keys [:topic-name
           :partition-count
           :replication-factor
           :topic-config]
    :as m}]
  (try
    (cond-> (NewTopic. ^String topic-name (int partition-count) (short replication-factor))
      topic-config (.configs (into {} (map (fn [[k v]]
                                             (assert (string? k))
                                             (assert (string? v))
                                             [k v]))
                                   topic-config)))
    (catch Exception e
      (throw (ex-info "While making NewTopic descriptor for topic" m e)))))

;;;; Result types

(defn->data DescribeClusterResult->data
  [^DescribeClusterResult dcr]
  {:cluster-id (-> dcr .clusterId .get)
   :controller (-> dcr .controller .get datafy)
   :nodes (->> dcr .nodes .get (mapv datafy))})
