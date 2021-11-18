(ns jackdaw.streams.specs
  ""
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.spec.alpha :as s]
            [jackdaw.specs]
            [jackdaw.streams :as k]
            [jackdaw.streams.lambdas :as lambdas]
            [jackdaw.streams.protocols
             :refer [IStreamsBuilder IGlobalKTable IKGroupedTable
                     IKGroupedStream IKStream IKTable
                     ITimeWindowedKStream ISessionWindowedKStream]])
  (:import org.apache.kafka.common.serialization.Serde
           org.apache.kafka.streams.kstream.JoinWindows))

(set! *warn-on-reflection* true)

(def global-ktable?
  (partial satisfies? IGlobalKTable))

(def join-windows?
  (partial instance? JoinWindows))

(def kgroupedstream?
  (partial satisfies? IKGroupedStream))

(def time-windowed-kstream?
  (partial satisfies? ITimeWindowedKStream))

(def session-windowed-kstream?
  (partial satisfies? ISessionWindowedKStream))

(def kgroupedtable?
  (partial satisfies? IKGroupedTable))

(def kstream?
  (partial satisfies? IKStream))

(def ktable?
  (partial satisfies? IKTable))

(def serde?
  (partial instance? Serde))

(def streams-builder?
  (partial satisfies? IStreamsBuilder))

(s/def ::max-records int?)
(s/def ::max-bytes int?)
(s/def ::until-time-limit-ms int?)
(s/def ::suppress-config (s/keys :opt-un [::max-records
                                          ::max-bytes
                                          ::until-time-limit-ms]))
(s/def ::topic-name string?)
(s/def ::key-serde any?)
(s/def ::value-serde any?)
(s/def ::partition-fn fn?)

(s/def ::topic-config
  (s/keys :req-un [::topic-name
                   ::key-serde
                   ::value-serde]
          :opt-un [::partition-fn]))

(s/def ::topic-configs (s/coll-of ::topic-config))

(s/def ::kstreams (s/coll-of kstream?))
(s/def ::kstream-or-ktable (s/or :kstream kstream? :ktable ktable?))

(s/def ::kgroupedstream-or-kgroupedtable
  (s/or :kgroupedstream kgroupedstream?
        :kgroupedtable kgroupedtable?
        :time-windowed time-windowed-kstream?
        :session-windowed session-windowed-kstream?))

;; IStreamsBuilder

(s/fdef k/kstreams
        :args (s/cat :streams-builder streams-builder?
                     :topic-configs ::topic-configs)
        :ret kstream?)

(s/fdef k/ktable
  :args (s/cat :streams-builder streams-builder?
               :topic-config ::topic-config
               :store-name (s/? string?))
  :ret ktable?)

(s/fdef k/global-ktable
        :args (s/cat :streams-builder streams-builder?
                     :topic-config ::topic-config
                     :store-name (s/? string?))
        :ret global-ktable?)

(s/fdef k/source-topics
        :args (s/cat :streams-builder streams-builder?)
        :ret (s/coll-of string? :kind set?))

(s/fdef k/streams-builder*
        :args (s/cat :streams-builder streams-builder?)
        :ret streams-builder?)

;; IKStreamBase

(s/fdef k/join
        :args (s/cat :kstream-or-ktable ::kstream-or-ktable
                     :ktable ktable?
                     :value-joiner-fn ifn?)
        :ret ::kstream-or-ktable)

(s/fdef k/left-join
        :args (s/cat :kstream-or-ktable ::kstream-or-ktable
                     :ktable ktable?
                     :value-joiner-fn ifn?
                     :this-topic-config (s/? ::topic-config)
                     :other-topic-config (s/? ::topic-config))
        :ret ::kstream-or-ktable)

(s/fdef k/for-each!
        :args (s/cat :kstream-or-ktable ::kstream-or-ktable
                     :foreach-fn ifn?))

(s/fdef k/filter
        :args (s/cat :kstream-or-ktable ::kstream-or-ktable
                     :predicate-fn ifn?)
        :ret ::kstream-or-ktable)

(s/fdef k/filter-not
        :args (s/cat :kstream-or-ktable ::kstream-or-ktable
                     :predicate-fn ifn?)
        :ret ::kstream-or-ktable)

(s/fdef k/group-by
        :args (s/cat :kstream-or-ktable ::kstream-or-ktable
                     :key-value-mapper-fn ifn?
                     :topic-config (s/? ::topic-config))
        :ret ::kgroupedstream-or-kgroupedtable)

(s/fdef k/peek
  :args (s/cat :kstream-or-ktable ::kstream-or-ktable
               :peek-fn ifn?)
  :ret ::kstream-or-ktable)


(s/fdef k/map-values
        :args (s/cat :kstream-or-ktable ::kstream-or-ktable
                     :value-mapper-fn ifn?)
        :ret ::kstream-or-ktable)

;; IKStream

(s/fdef k/branch
        :args (s/cat :kstream kstream?
                     :predicate-fns (s/coll-of ifn?))
        :ret (s/coll-of kstream?))

(s/fdef k/flat-map
        :args (s/cat :kstream kstream?
                     :key-value-mapper-fn ifn?)
        :ret kstream?)

(s/fdef k/print!
        :args (s/cat :kstream kstream?
                     :topic-config (s/? ::topic-config)))

(s/fdef k/through
        :args (s/cat :kstream kstream?
                     :topic-config ::topic-config)
        :ret kstream?)

(s/fdef k/to!
        :args (s/cat :kstream kstream?
                     :topic-config ::topic-config))

(s/fdef k/flat-map-values
        :args (s/cat :kstream kstream?
                     :value-mapper-fn ifn?)
        :ret kstream?)

(s/fdef k/group-by-key
        :args (s/cat :kstream kstream?
                     :topic-config (s/? ::topic-config))
        :ret kgroupedstream?)

(s/fdef k/join-windowed
        :args (s/cat :kstream kstream?
                     :other-kstream kstream?
                     :value-joiner-fn ifn?
                     :windows join-windows?
                     :this-topic-config (s/? ::topic-config)
                     :other-topic-config (s/? ::topic-config))
        :ret kstream?)

(s/fdef k/left-join-windowed
        :args (s/cat :kstream kstream?
                     :other-kstream kstream?
                     :value-joiner-fn ifn?
                     :windows join-windows?
                     :this-topic-config (s/? ::topic-config)
                     :other-topic-config (s/? ::topic-config))
        :ret kstream?)

(s/fdef k/map
        :args (s/cat :kstream kstream?
                     :key-value-mapper-fn ifn?)
        :ret kstream?)

(s/fdef k/merge
        :args (s/cat :kstream kstream?
                     :other kstream?)
        :ret kstream?)

(s/fdef k/outer-join-windowed
        :args (s/cat :kstream kstream?
                     :other-kstream kstream?
                     :value-joiner-fn ifn?
                     :windows join-windows?
                     :this-topic-config (s/? ::topic-config)
                     :other-topic-config (s/? ::topic-config))
        :ret kstream?)

(s/fdef k/process!
        :args (s/cat :kstream kstream?
                     :processor-fn ifn?
                     :state-store-names (s/coll-of string?)))

(s/fdef k/select-key
        :args (s/cat :kstream kstream?
                     :select-key-value-mapper-fn ifn?)
        :ret kstream?)

(s/fdef k/transform
        :args (s/cat :kstream kstream?
                     :transformer-supplier-fn ifn?
                     :state-store-names (s/? (s/coll-of string?)))
        :ret kstream?)

(s/fdef k/flat-transform
        :args (s/cat :kstream kstream?
                     :transformer-supplier-fn ifn?
                     :state-store-names (s/? (s/coll-of string?)))
        :ret kstream?)

(s/fdef k/transform-values
        :args (s/cat :kstream kstream?
                     :value-transformer-supplier-fn ifn?
                     :state-store-names (s/? (s/coll-of string?)))
        :ret kstream?)

(s/fdef k/flat-transform-values
        :args (s/cat :kstream kstream?
                     :value-transformer-supplier-fn ifn?
                     :state-store-names (s/? (s/coll-of string?)))
        :ret kstream?)

(s/fdef k/join-global
        :args (s/cat :kstream kstream?
                     :global-ktable global-ktable?
                     :kv-mapper ifn?
                     :joiner ifn?)
        :ret kstream?)

(s/fdef k/left-join-global
        :args (s/cat :kstream kstream?
                     :global-ktable global-ktable?
                     :kv-mapper ifn?
                     :joiner ifn?)
        :ret kstream?)

(s/fdef k/kstream*
        :args (s/cat :kstream kstream?)
        :ret kstream?)

;; IKTable

(s/fdef k/outer-join
        :args (s/cat :ktable ktable?
                     :other-ktable ktable?
                     :value-joiner-fn ifn?)
        :ret ktable?)

(s/fdef k/suppress
  :args (s/cat :ktable ktable?
               :suppress ::suppress-config)
  :ret ktable?)

(s/fdef k/to-kstream
        :args (s/cat :ktable ktable?
                     :key-value-mapper-fn (s/? ifn?))
        :ret kstream?)

(s/fdef k/ktable*
        :args (s/cat :ktable ktable?)
        :ret ktable?)

;; IKGroupBase

(s/fdef k/aggregate
        :args (s/cat :kgrouped ::kgroupedstream-or-kgroupedtable
                     :initializer-fn ::lambdas/initializer-fn
                     :adder-fn ::lambdas/aggregator-fn
                     :subtractor-or-merger-fn (s/? (s/alt ::lambdas/aggregator-fn ::lambdas/merger-fn))
                     :topic-config (s/? ::topic-config))
        :ret ktable?)

(s/fdef k/count
        :args (s/cat :kgrouped ::kgroupedstream-or-kgroupedtable
                     :topic-config ::topic-config)
        :ret ktable?)

(s/fdef k/reduce
        :args (s/cat ::kgrouped ::kgroupedstream-or-kgroupedtable
                     :adder-or-reducer-fn ifn?
                     :subtractor-fn (s/? ifn?)
                     :topic-config (s/? ::topic-config))
        :ret ktable?)

;; IKGroupedTable

(s/fdef k/kgroupedtable*
        :args (s/cat :kgroupedtable kgroupedtable?)
        :ret kgroupedtable?)

;; IKGroupedStream

(s/fdef k/aggregate-window
        :args (s/cat :kgroupedstream kgroupedstream?
                     :initializer-fn ::lambdas/initializer-fn
                     :aggregator-fn ::lambdas/aggregator-fn
                     :windows join-windows?
                     :topic-config ::topic-config)
        :ret ktable?)

(s/fdef k/count-windowed
        :args (s/cat :kgroupedstream kgroupedstream?
                     :windows join-windows?
                     :topic-config ::topic-config)
        :ret ktable?)

(s/fdef k/reduce-windowed
        :args (s/cat :kgroupedstream kgroupedstream?
                     :reducer-fn ifn?
                     :windows join-windows?
                     :topic-config ::topic-config)
        :ret ktable?)

(s/fdef k/kgroupedstream*
        :args (s/cat :kgroupedstream kgroupedstream?)
        :ret kgroupedstream?)

;; IGlobalKTable

(s/fdef k/global-ktable*
        :args (s/cat :global-ktable global-ktable?)
        :ret global-ktable?)
