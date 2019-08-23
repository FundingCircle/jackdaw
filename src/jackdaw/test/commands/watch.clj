(ns jackdaw.test.commands.watch
  (:require
   [jackdaw.test.journal :as j]
   [clojure.tools.logging :as log]))

;; Circle CI times out after a minute, so make sure this is less than 60,000
(def ^:dynamic *default-watch-timeout* 10000)

(defn- watch-timeout [t]
  (if (or (nil? t) (= t :default-timeout))
    *default-watch-timeout*
    t))

(defn- watch-params
  ([watch-query] [watch-query "Watcher" *default-watch-timeout*])
  ([watch-query opts] [watch-query (:info opts) (watch-timeout (:timeout opts))]))

(defn handle-watch-cmd
  [machine params]
  (let [[query info timeout] (apply watch-params params)
        condition? query]
    (j/watch-for machine condition? timeout info)))

(def command-map
  {:watch handle-watch-cmd})
