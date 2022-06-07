(ns jackdaw.test.commands.base
  (:require
   [clojure.pprint :as pprint]))

(set! *warn-on-reflection* true)

(def command-map
  {:stop (constantly true)

   :sleep (fn [_machine [sleep-ms]]
            (Thread/sleep sleep-ms))

   :println (fn [_machine params]
              (println (apply str params)))

   :pprint (fn [_machine params]
              (pprint/pprint params))

   :do (fn [machine [do-fn]]
         (do-fn @(:journal machine)))

   :do! (fn [machine [do-fn]]
          (do-fn (:journal machine)))

   :inspect (fn [machine [inspect-fn]]
              (inspect-fn machine))})
