(ns jackdaw.admin.client
  (:require [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s])
  (:import [org.apache.kafka.clients.admin
            AdminClient
            Config
            ConfigEntry
            DescribeTopicsOptions
            DescribeTopicsResult
            NewTopic]
           [org.apache.kafka.common.config
            ConfigResource
            ConfigResource$Type]))

;; copied from jackdaw.client to simplify deps tree
(defn map->properties [m]
  (let [props (Properties.)]
    (when m
      (.putAll props (stringify-keys m)))
    props))

(defn client [kafka-config]
  {:pre [(get kafka-config "bootstrap.servers")]}
  (AdminClient/create (map->properties kafka-config)))

(defn client? [x]
  (instance? AdminClient x))

(defn list-topics [client]
  (-> client
      .listTopics
      .names
      .get))

(defn describe-topics [client names]
  (as-> client %
      (.describeTopics % names (DescribeTopicsOptions.))
      (.all %)
      (deref %)
      (map (fn [[name topic-description]]
             [name {:is-internal? (.isInternal topic-description)
                    :partition-info (map (fn [part-info]
                                           {:isr (.isr part-info)
                                            :leader (.leader part-info)
                                            :partition (.partition part-info)
                                            :replicas (.replicas part-info)})
                                         (.partitions topic-description))}])
           %)
      (into {} %)))

(defn config-resource? [x]
  (instance? ConfigResource x))

(s/def :config.resource/value? any?)
(s/def :config.resource/default? boolean?)
(s/def :config.resource/read-only? boolean?)

(s/fdef config->map-all
  :args (s/cat :cr config-resource?)
  :ret (s/map-of string?
                 (s/keys :req-un [:config.resource/value?
                                  :config.resource/default?
                                  :config.resource/read-only?])))

(defn config->map-all [^ConfigResource config]
  (->> config
       .entries
       seq
      (map (fn [^ConfigEntry e]
             [(.name e)
              {:value (.value e)
               :default? (.isDefault e)
               :read-only? (.isReadOnly e)
               :sensitive? (.isSensitive e)}]))
      (into {})))

(s/fdef config->map-values
  :args (s/cat :cr config-resource?)
  :ret (s/map-of string? :config.resource/value?))

(defn config->map-values [^ConfigResource config]
  (-> config
      .entries
      seq
      (->>
       (map (fn [^ConfigEntry e]
              [(.name e) (.value e)]))
       (into {}))))

(s/fdef describe-topic-config
  :args (s/cat :client client? :topic-name string?)
  :ret (s/map-of string? string?))

(defn describe-topic-config [client topic-name]
  (-> client
      (.describeConfigs [(ConfigResource. ConfigResource$Type/TOPIC topic-name)])
      .all
      deref
      vals
      first
      config->map-values))

(defn existing-topic-names [client]
  (-> client .listTopics .names deref))

(defn topic-exists? [client name]
  (contains? (existing-topic-names client) name))

(defn create-topic-
  [client topic-name num-partitions replication-factor topic-config]
  (let [topic (NewTopic. topic-name num-partitions replication-factor)]
    (.configs topic (map->properties topic-config))
    (-> client
        (.createTopics [topic])
        .all
        deref))
  (log/info (format "Created topic %s" topic-name)))

(defn create-topics! [client topic-metadata]
  (doseq [{topic-name :jackdaw.topic/topic-name
           partitions :jackdaw.topic/partitions
           replication-factor :jackdaw.topic/replication-factor
           topic-config :jackdaw.topic/topic-config} topic-metadata
          :when (not (topic-exists? client topic-name))]
    (create-topic- client
                   topic-name
                   (int partitions)
                   (int replication-factor)
                   topic-config)))

(defn topics-ready?
  "Waits until all topics and partitions are ready (have a leader, in-sync-replicas)"
  [client names]
  (->> (describe-topics client names)
       (every? (fn [topic]
                 (every? (fn [part-info]
                           (and (boolean (:leader part-info))
                                (seq (:isr part-info))))
                         (:partition-info topic))))))

(s/fdef map->config :args (s/cat :m (s/map-of string? string?)))
(defn map->config
  [config-map]
  (->> config-map
       (mapv (fn [[k v]]
               (ConfigEntry. k v)))
       (Config. )))

(s/fdef alter-topic-config! :args (s/cat :ac :client? :name :topic/name :config (s/map-of string? string?)))
(defn alter-topic-config! [client topic-name config]
  (-> client
      (.alterConfigs {(ConfigResource. ConfigResource$Type/TOPIC topic-name) (map->config config)})
      .all
      deref))

(defn node->map [node]
  {:host (.host node)
   :port (.port node)
   :id (.id node)
   :rack (.rack node)})

(defn describe-cluster [client]
  (let [result (-> client
                   (.describeCluster))]
    {:cluster-id (-> result .clusterId .get)
     :controller (-> result .controller .get)
     :nodes (-> result .nodes .get (->> (map node->map) vec))}))

(defn get-broker-config
  "Returns the broker config as a map.

  Broker-id is an int, typically 0-2, get the list of valid broker ids
  using describe-cluster"
  [client broker-id]
  {:pre [(client? client)]}
  (-> client
      (.describeConfigs [(ConfigResource. ConfigResource$Type/BROKER (str broker-id))])
      .all
      .get
      vals
      first
      config->map-all))
