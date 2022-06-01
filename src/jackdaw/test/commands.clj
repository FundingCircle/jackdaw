(ns jackdaw.test.commands
  (:require
   [clojure.spec.alpha :as s]
   [jackdaw.test.commands.base :as base]
   [jackdaw.test.commands.write :as write]
   [jackdaw.test.commands.watch :as watch])
  (:refer-clojure :exclude [do]))

(set! *warn-on-reflection* true)

(def base-commands base/command-map)
(def write-command write/command-map)
(def watch-command watch/command-map)

(def command-map
  (merge base-commands
         write-command
         watch-command))

(defn command-handler
  [machine cmd]
  (let [[cmd & params] cmd
        handler (get command-map cmd)]

    (if handler
      ;; Happy
      (let [result (handler machine params)]
        (assoc {}
               :result result
               :cmd cmd
               :params params))
      ;; else Sad
      (throw (ex-info (format "Unknown command: %s" cmd)
                      {:cmd cmd
                       :error :unknown-command
                       :params params
                       :available-commands (keys command-map)})))))

(defn with-handler
  [machine handler]
  (assoc machine
         :command-handler handler))

;; Test Command API

(s/def ::topic-id (s/or :keyword keyword?
                        :string string?))
(s/def ::test-message any?)
(s/def ::write-options map?)
(s/def ::watch-options map?)

(defn do
  "Invoke the provided function, passing a snapshot of the test journal

   Use this to perform side-effects without representing their result in the journal"
  [do-fn]
  `[:do ~do-fn])

(s/fdef do
  :args ifn?
  :ret vector?)

(defn do!
  "Invoke the provided function, passing the journal `ref`

   Use this to perform side-effects when you want to represent the result in the journal
   (e.g. insert test-data into an external database AND into the journal with the expectation
   that it will eventually appear in kafka via some external system like kafka-connect)"
  [do-fn]
  `[:do! ~do-fn])

(s/fdef do!
  :args ifn?
  :ret vector?)

(defn write!
  "Write a message to the topic identified in the topic-metadata by `topic-id`

   `:message` is typically a map to be serialized by the Serde configured in the topic-metadata
              but it can be whatever the configued Serde is capable of serializing
   `:options` is an optional map containing additional information describing how the test-message
              should be created. The following properties are supported.

      `:key`             An explicit key to associate with the test message
      `:key-fn`          A function to derive the key from the test message
      `:partition`       The partition to which the test message should be written
      `:partition-fn`    A function to derive the partition to which the test message should be written"
  ([topic-id message]
   `[:write! ~topic-id ~message])

  ([topic-id message options]
   `[:write! ~topic-id ~message ~options]))

(s/fdef write!
  :args (s/cat :topic-id ::topic-id
               :message ::test-message
               :options (s/? ::write-options))
  :ret vector?)

(defn watch
  "Watch the test-journal until the `watch-fn` predicate returns true

   `:watch-fn` is a function that takes the journal and returns true once the journal
               contains evidence of the test being complete
   `:options` is an optional map containing additional information describing how the watch
              function will be run. The following properties are supported.

      `:info` Diagnostic information to be included in the response when a watch fails
              to observe the expected data in the journal
      `:timeout` The number of milliseconds to wait before giving up"
  ([watch-fn]
   `[:watch ~watch-fn])
  ([watch-fn options]
   `[:watch ~watch-fn ~options]))

(s/fdef watch
  :args (s/cat :watch-fn ifn?
               :options (s/? ::watch-options))
  :ret vector?)
