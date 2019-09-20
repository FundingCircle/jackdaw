(ns jackdaw.streams.describe
  (:require [clj-uuid :as uuid]
            [clojure.string :as str]))

(defn ->edge
  [from to]
  {:from from :to to})

(defn base-node
  [t n]
  {:nodes [{:type t
            :name (.name n)}]
   :edges (concat
           (map #(->edge (.name %) (.name n)) (.predecessors n))
           (map #(->edge (.name n) (.name %)) (.successors n)))})

(defn describe-node-dispatch
  [n]
  (keyword (str/lower-case (.getSimpleName (.getClass n)))))

(defmulti describe-node describe-node-dispatch)

(defmethod describe-node :node [n]
  (base-node :node n))

(defmethod describe-node :source [n]
  (let [topics (map str/trim (-> (.topics n)
                                 (str/replace "[" "")
                                 (str/replace "]" "")
                                 (str/split #",")))]
    (-> (base-node :source n)
        (update :nodes concat (map (fn [t]
                                     {:type :topic
                                      :name t}) topics))
        (update :edges concat (map #(->edge % (.name n)) topics)))))

(defmethod describe-node :sink [n]
  (-> (base-node :sink n)
      (update :nodes conj {:type :topic
                           :name (.topic n)})
      (update :edges conj (->edge (.name n) (.topic n)))))

(defmethod describe-node :processor [n]
  (let [stores (.stores n)]
    (-> (base-node :processor n)
        (update :nodes concat (map (fn [t]
                                     {:type :store
                                      :name (str "localstore-" t)}) stores))
        (update :edges concat (map #(->edge (.name n) (str "localstore-" %)) stores)))))

(defmethod describe-node :globalstore [n]
  (let [source (describe-node (.source n))
        processor (describe-node (.processor n))]
    {:type :globalstore
     :name (str "globalstore-" (.id n))
     :nodes (set (mapcat :nodes [source processor]))
     :edges (set (mapcat :edges [source processor]))}))

(defmethod describe-node :subtopology [n]
  (let [nodes (map describe-node (.nodes n))]
    {:type :stream
     :name (str "stream-" (.id n))
     :nodes (set (mapcat :nodes nodes))
     :edges (set (mapcat :edges nodes))}))

(defn topic?
  [s]
  (= :topic (:type s)))

(defn gen-id
  [applicaton-id n]
  ;; Take a base UUID from the application id, or a global one for topics
  (let [ns-id (uuid/v5 uuid/+null+ (if (topic? n)
                                     "topics" ; topics are global
                                     applicaton-id))]
    ;; generate a deterministic v5 UUID for the node name for this applicaton-id
    ;; means the same node in the same app gets the same id, but a node with the
    ;; same name in a different app gets a non matching UUID.
    ;; This is required so we can merge the graphs across applicaton-ids.
    (uuid/v5 ns-id (:name n))))

(defn assign-id
  [applicaton-id n]
  (assoc n :id (gen-id applicaton-id n)))

(defn assign-ids
  [applicaton-id g]
  (let [g* (-> (update g :nodes (fn [v]
                                  (map (partial assign-id applicaton-id) v)))
               (assoc :id (gen-id applicaton-id g)))
        lookup (into {} (map (fn [v]
                               [(:name v) v]) (:nodes g*)))]
    (update g* :edges (fn [v]
                        (map (fn [e]
                               (assoc e
                                      :from-id (:id (lookup (:from e)))
                                      :to-id (:id (lookup (:to e))))) v)))))

(defn is-merge?
  [n]
  (str/starts-with? n "KSTREAM-MERGE"))

(defn good-edge
  [e]
  (not= (:from-id e) (:to-id e)))

(defn collapse-merge-chains
  [g]
  ;; all kafka merges are pairwise, so if you merge lots of topics the graph ends up with
  ;; a long chain of merges all in a row which is messy. This collapses chains of pair-wise
  ;; merges into a single N-way merge
  (let [merge-to-merge-edges (filter (fn [{:keys [from to]}]
                                       (and (is-merge? from)
                                            (is-merge? to))) (:edges g))
        start-id (:from-id (first
                            (filter (fn [{:keys [from-id]}]
                                      (not-any? (fn [{:keys [to-id]}]
                                                  (= from-id to-id)) merge-to-merge-edges))
                                    merge-to-merge-edges)))
        ;; Collapse all the merge nodes in the chain into the merge node at the head of the starting edge.
        ;; All references from edges to the 'collapsed' nodes need to be changed to the head node.
        ;; Remove any pruned nodes and edges as a result.

        ;; generate remappings
        remappings (into {} (map (fn [v]
                                   [v start-id])
                                 (remove #(= % start-id)
                                         (mapcat (fn [{:keys [from-id to-id]}]
                                                   [from-id to-id]) merge-to-merge-edges))))
        pruned-ids (set (keys remappings))]

    (-> g
        (update :edges (fn [edges]
                         (filter good-edge
                                 (map (fn [{:keys [from-id to-id] :as e}]
                                        (assoc e
                                               :from-id (remappings from-id from-id)
                                               :to-id (remappings to-id to-id)))
                                      edges))))
        (update :nodes (fn [nodes]
                         (filter (fn [n]
                                   (not (contains? pruned-ids (:id n))))
                                 nodes))))))

(defn parse-description
  [applicaton-id d]
  (let [parser (comp collapse-merge-chains
                     (partial assign-ids applicaton-id)
                     describe-node)]
    (concat (map parser (.subtopologies d))
            (map parser (.globalStores d)))))

(defn describe-topology
  "Returns a list of the stream graphs in a topology.
  The passed in topology object must have a `describe` method, meaning
  it is one of:

  Kafka >= 1.1 : https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/Topology.html
  Kafka <  1.1 : https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/processor/TopologyBuilder.html#internalTopologyBuilder

  Each stream graph takes the form:

  {:id    <a unique UUID for the stream, deterministic from the encosing topology and its stream name>
   :type  :stream
   :name  <the name that kafka gives this stream>
   :nodes <a list of all the nodes in the graph>
   :edges <a list of all the edges in the graph>}

  Nodes and edges are represented as:

  {:id   <a deterministic UUID for the node>
   :name <the name as assigned by kafka>
   :type <the type - processor, store, topic &c.>}

  {:from    <the :name of the node the edge comes from>
   :from-id <the :id of the node the edge comes from>
   :to      <the :name of the node the edge goes to>
   :to-id   <the :id of the node the edge goes to>}

  All identifiers are v5 UUIDs, and are globally unique where objects
  are distinct and globally equal where objects are the same."
  [topology streams-config]
  (parse-description (get streams-config "application.id") (.describe topology)))
