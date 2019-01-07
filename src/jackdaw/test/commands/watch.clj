(ns jackdaw.test.commands.watch
  (:require
   [jackdaw.test.journal :as j]
   [clojure.tools.logging :as log]))

;; Circle CI times out after a minute, so make sure this is less than 60,000
(def ^:dynamic *default-watch-timeout* 45000)

(defn- watch-timeout [t]
  (if (= t :default-timeout)
    *default-watch-timeout*
    t))

(defn- watch-params
  ([watch-query] [*default-watch-timeout* watch-query "Watcher"])
  ([timeout watch-query] [(watch-timeout timeout) watch-query "Watcher"])
  ([timeout watch-query info] [(watch-timeout timeout) watch-query info]))

(defn handle-watch-cmd
  [machine cmd params]
  (let [[timeout query info] (apply watch-params params)
        condition? (fn [journal]
                     (query journal))
        journal (:journal machine)]
    (j/watch-for machine condition? timeout info)))

(def command-map
  {:jackdaw.test.commands/watch! handle-watch-cmd})
