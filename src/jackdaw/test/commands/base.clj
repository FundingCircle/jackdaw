(ns jackdaw.test.commands.base
  (:require
    [clojure.pprint :as pprint]))

(def command-map
  {:stop (constantly true)

   :sleep (fn [machine [sleep-ms]]
            (Thread/sleep sleep-ms))

   :println (fn [machine params]
              (println (apply str params)))

   :pprint (fn [machine params]
              (pprint/pprint params))

   :do (fn [machine [do-fn]]
         (do-fn @(:journal machine)))

   :do! (fn [machine [do-fn]]
          (do-fn (:journal machine)))

   :inspect (fn [machine [inspect-fn]]
              (inspect-fn machine))})
