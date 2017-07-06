(ns jackdaw.streams.lambdas.specs
  (:require [clojure.spec.alpha :as s]
            [jackdaw.streams.lambdas :as lambdas])
  (:import [org.apache.kafka.streams.kstream
            Aggregator Initializer]))

(def initializer? (partial instance? Initializer))
(def aggregator? (partial instance? Aggregator))

(s/def ::lambdas/initializer-fn ifn?)
(s/def ::lambdas/aggregator-fn ifn?)

(s/fdef lambdas/initializer
        :args (s/cat :initializer-fn ::lambdas/initializer-fn)
        :ret initializer?)

(s/fdef lambdas/aggregator
        :args (s/cat :aggregator-fn ::lambdas/aggregator-fn)
        :ret aggregator?)
