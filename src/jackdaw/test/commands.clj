(ns jackdaw.test.commands
  ""
  (:require
   [clojure.spec.alpha :as s]
   [jackdaw.test.commands.base :as base]
   [jackdaw.test.commands.write :as write]
   [jackdaw.test.commands.watch :as watch])
  (:refer-clojure :exclude [do]))

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

(s/def ::topic-id (s/or ::keyword keyword?
                        ::string string?))
(s/def ::test-message any?)
(s/def ::write-options map?)
(s/def ::watch-options map?)

(defn do [do-fn]
  `[:do ~do-fn])

(s/fdef do
  :args ifn?
  :ret vector?)

(defn do! [do-fn]
  `[:do! ~do-fn])

(s/fdef do!
  :args ifn?
  :ret vector?)

(defn write!
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
  ([watch-query]
   `[:watch ~watch-query])
  ([watch-query options]
   `[:watch ~watch-query ~options]))

(s/fdef watch
  :args (s/cat :watch-query ifn?
               :options (s/? ::watch-options))
  :ret vector?)
