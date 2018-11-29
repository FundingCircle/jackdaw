(in-ns 'jackdaw.data)

(import '[org.apache.kafka.common
          Node TopicPartition TopicPartitionInfo])

;;; Node

(defn->data Node->data
  ""
  [^Node node]
  {:host (.host node)
   :port (.port node)
   :id (.id node)
   :rack (.rack node)})

;;; TopicPartitionInfo

(defn->data TopicPartitionInfo->data
  ""
  [^TopicPartitionInfo tpi]
  {:isr (mapv datafy (.isr tpi))
   :leader (datafy (.leader tpi))
   :partition (.partition tpi)
   :replicas (mapv datafy (.replicas tpi))})

;;; Topic partition tuples

(defn ^TopicPartition ->TopicPartition
  "Given unrolled ctor-style arguments, create a Kafka `TopicPartition`."
  [{:keys [:topic-name]} partition]
  (TopicPartition. topic-name (int partition)))

(defn map->TopicPartition
  "Given a `::topic-parititon`, build an equivalent `TopicPartition`.

  Inverts `(datafy ^TopicPartition tp)`."
  [{:keys [:topic-name
           :partition]
    :as m}]
  (->TopicPartition m partition))

(defn->data TopicPartition->data [^TopicPartition tp]
  {:topic-name (.topic tp)
   :partition (.partition tp)})

(defn as-TopicPartition
  ""
  ^TopicPartition [o]
  (cond (instance? TopicPartition o)
        o

        (map? o)
        (or (:clojure.datafy/obj (meta o))
            (map->TopicPartition o))

        :else
        (throw (ex-info "Unable to build TopicPartition"
                        {:o o
                         :class (class o)}))))

(comment
  (->TopicPartition {:topic-name "foo"} 1)
  (TopicPartition->data *1)
  (map->TopicPartition *1)
  ;; On 1.10+
  (datafy *1))
