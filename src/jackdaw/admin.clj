(ns jackdaw.admin
  "Tools for administering or just interacting with a Kafka cluster.

  Wraps the `AdminClient` API, replacing the Scala admin APIs.

  Like the underlying `AdminClient` API, this namespace is subject to
  change and should be considered of alpha stability."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require
   [jackdaw.data :as jd]
   [manifold.deferred :as d])
  (:import [java.util Properties]
           [org.apache.kafka.clients.admin AdminClient
            DescribeTopicsOptions DescribeClusterOptions DescribeConfigsOptions]))

(set! *warn-on-reflection* true)

(defprotocol Client
  (alter-topics* [this topics])
  (create-topics* [this topics])
  (delete-topics* [this topics])
  (describe-topics* [this topics])
  (describe-configs* [this configs])
  (describe-cluster* [this])
  (list-topics* [this]))

(def client-impl
  {:alter-topics* (fn [this topics]
                    (d/future
                      @(.all (.alterConfigs ^AdminClient this topics))))
   :create-topics* (fn [this topics]
                    (d/future
                      @(.all (.createTopics ^AdminClient this topics))))
   :delete-topics*  (fn [this topics]
                      (d/future
                        @(.all (.deleteTopics ^AdminClient this topics))))
   :describe-topics* (fn [this topics]
                       (d/future
                         @(.all (.describeTopics ^AdminClient this topics (DescribeTopicsOptions.)))))
   :describe-configs* (fn [this configs]
                        (d/future
                          @(.all (.describeConfigs ^AdminClient this configs (DescribeConfigsOptions.)))))
   :describe-cluster* (fn [this]
                        (d/future
                          (jd/datafy (.describeCluster ^AdminClient this (DescribeClusterOptions.)))))
   :list-topics* (fn [this]
                   (d/future
                     @(.names (.listTopics ^AdminClient this))))})

(extend AdminClient
  Client
  client-impl)

(defn ->AdminClient
  "Given a Kafka properties map having `\"bootstrap.servers\"`, return
  an `AdminClient` bootstrapped off of the configured servers."
  ^AdminClient [kafka-config]
  {:pre [(get kafka-config "bootstrap.servers")]}
  (AdminClient/create ^Properties (jd/map->Properties kafka-config)))

(defn client?
  "Predicate.

  Return `true` if and only if given an `AdminClient` instance."
  [x]
  (instance? AdminClient x))

(defn list-topics
  "Given an `AdminClient`, return a seq of topic records, being the
  topics on the cluster."
  [^AdminClient client]
  {:pre [(client? client)]}
  (->> @(list-topics* client)
       ;; We should allow the caller to decide whether they want
       ;; the result to be sorted or not?
       sort
       (map #(hash-map :topic-name %))))

(defn topic-exists?
  "Verifies the existence of the topic.

  Does not verify any config. details or values."
  [^AdminClient client {:keys [topic-name] :as topic}]
  {:pre [(client? client)
         (string? topic-name)]}
  (contains? (set (list-topics client)) {:topic-name topic-name}))

(defn retry-exists?
  "Returns `true` if topic exists. Otherwise spins as configured."
  [client topic num-retries wait-ms]
  (cond (topic-exists? client topic)
        true

        (zero? num-retries)
        false

        :else
        (do (Thread/sleep wait-ms)
            (recur client topic (dec num-retries) wait-ms))))

(defn create-topics!
  "Given an `AdminClient` and a collection of topic descriptors,
  create the specified topics with their configuration(s).

  Does not block until the created topics are ready. It may take some
  time for replicas and leaders to be chosen for newly created
  topics.

  See `#'topics-ready?`, `#'topic-exists?` and `#'retry-exists?` for
  tools with which to wait for topics to be ready."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  @(create-topics* client (map jd/map->NewTopic topics)))

(defn describe-topics
  "Given an `AdminClient` and an optional collection of topic
  descriptors, return a map from topic names to topic
  descriptions.

  If no topics are provided, describes all topics.

  Note that the topic description does NOT include the topic's
  configuration.See `#'describe-topic-config` for that capability."
  ([^AdminClient client]
   {:pre [(client? client)]}
   (describe-topics client (list-topics client)))
  ([^AdminClient client topics]
   {:pre [(client? client)
          (sequential? topics)]}
   (->> @(describe-topics* client (map :topic-name topics))
        (map (fn [[k v]] [k (jd/datafy v)]))
        (into {}))))

(defn describe-topics-configs
  "Given an `AdminClient` and a collection of topic descriptors, returns
  the selected topics' live configuration as a map from topic names to
  configured properties to metadata about each property including its
  current value."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  (->> @(describe-configs* client (map #(-> % :topic-name jd/->topic-resource) topics))
       (into {})
       (reduce-kv (fn [m k v]
                    (assoc m (jd/datafy k) (jd/datafy v)))
                  {})))

(defn topics-ready?
  "Given an `AdminClient` and a sequence topic descriptors, return
  `true` if and only if all listed topics have a leader and in-sync
  replicas.

  This can be used to determine if some set of newly created topics
  are healthy yet, or detect whether leader re-election has finished
  following the demise of a Kafka broker."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  (->> @(describe-topics* client (map :topic-name topics))
       (every? (fn [[_topic-name {:keys [partition-info]}]]
                 (every? (fn [part-info]
                           (and (boolean (:leader part-info))
                                (seq (:isr part-info))))
                         partition-info)))))

(defn- topics->configs
  ^java.util.Map [topics]
  (into {}
        (map (fn [{:keys [topic-name topic-config] :as t}]
               {:pre [(string? topic-name)
                      (map? topic-config)]}
               [(jd/->ConfigResource jd/+topic-config-resource-type+
                                     topic-name)
                (jd/map->Config topic-config)]))
        topics))

(defn alter-topic-config!
  "Given an `AdminClient` and a sequence of topic descriptors having
  `:topic-config`, alters the live configuration of the specified
  topics to correspond to the specified `:topic-config`."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  @(alter-topics* client (topics->configs topics)))

(defn delete-topics!
  "Given an `AdminClient` and a sequence of topic descriptors, marks the
  topics for deletion.

  Does not block until the topics are deleted, just until the deletion
  request(s) are acknowledged."
  [^AdminClient client topics]
  {:pre [(client? client)
         (sequential? topics)]}
  @(delete-topics* client (map :topic-name topics)))

(defn partition-ids-of-topics
  "Given an `AdminClient` and an optional sequence of topics, produces a
  mapping from topic names to a sequence of the partition IDs for that
  topic.

  By default, enumerates the partition IDs for all topics."
  ([^AdminClient client]
   {:pre [(client? client)]}
   (partition-ids-of-topics client (list-topics client)))
  ([^AdminClient client topics]
   {:pre [(client? client)
          (sequential? topics)]}
   (->> (describe-topics client topics)
        (map (fn [[topic-name {:keys [partition-info]}]]
               [topic-name (mapv :partition partition-info)]))
        (into {}))))

(defn describe-cluster
  "Returns a `DescribeClusterResult` describing the cluster."
  [^AdminClient client]
  {:pre [(client? client)]}
  (-> @(describe-cluster* client)
      jd/datafy))

(defn get-broker-config
  "Returns the broker config as a map.

  Broker-id is an int, typically 0-2, get the list of valid broker ids
  using describe-cluster"
  [^AdminClient client broker-id]
  {:pre [(client? client)]}
  (-> @(describe-configs* client [(jd/->broker-resource (str broker-id))])
      vals first jd/datafy))
