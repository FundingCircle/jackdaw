(ns jackdaw.test.commands
  ""
  (:require
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

(defn do [do-fn]
  `[:do ~do-fn])

(defn do! [do-fn]
  `[:do! ~do-fn])

(defn write!
  ([topic-id message]
   `[:write! ~topic-id ~message])

  ([topic-id message options]
   `[:write! ~topic-id ~message ~options]))

(defn watch
  ([watch-query]
   `[:watch ~watch-query])
  ([watch-query opts]
   `[:watch ~watch-query ~opts]))
