(ns jackdaw.streams.lambdas.specs
  "FIXME"
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.spec.alpha :as s]
            [jackdaw.streams.lambdas :as lambdas])
  (:import [org.apache.kafka.streams.kstream
            Aggregator Initializer Merger]))

(def initializer? (partial instance? Initializer))
(def aggregator? (partial instance? Aggregator))
(def merger? (partial instance? Merger))

(s/def ::lambdas/initializer-fn ifn?)
(s/def ::lambdas/aggregator-fn ifn?)
(s/def ::lambdas/merger-fn ifn?)

(s/fdef lambdas/initializer
        :args (s/cat :initializer-fn ::lambdas/initializer-fn)
        :ret initializer?)

(s/fdef lambdas/aggregator
        :args (s/cat :aggregator-fn ::lambdas/aggregator-fn)
        :ret aggregator?)

(s/fdef lambdas/merger
        :args (s/cat :merger-fn ::lambdas/merger-fn)
        :ret merger?)
