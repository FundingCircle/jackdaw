(ns jackdaw.test.commands.base
  (:require
    [clojure.pprint :as pprint]))

(def command-map
  {:stop (constantly true)

   :sleep (fn [machine cmd [sleep-ms]]
            (Thread/sleep sleep-ms))

   :println (fn [machine cmd params]
              (println (apply str params)))

   :pprint (fn [machine cmd params]
              (pprint/pprint params))

   :do (fn [machine cmd [do-fn]]
         (do-fn @(:journal machine)))

   :do! (fn [machine cmd [do-fn]]
          (do-fn (:journal machine)))

   :inspect (fn [machine cmd [inspect-fn]]
              (inspect-fn machine))})
