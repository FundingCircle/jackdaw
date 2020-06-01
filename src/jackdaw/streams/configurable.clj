(ns jackdaw.streams.configurable
  "Protocol for a configurable thing."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"})

(set! *warn-on-reflection* true)

(defprotocol IConfigurable
  (config [_] "Gets the configuration.")
  (configure [_ key value] "Adds a configuration."))
