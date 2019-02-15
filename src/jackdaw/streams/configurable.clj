(ns jackdaw.streams.configurable
  "Protocol for a configurable thing.")

(defprotocol IConfigurable
  (config [_] "Gets the configuration.")
  (configure [_ key value] "Adds a configuration."))
