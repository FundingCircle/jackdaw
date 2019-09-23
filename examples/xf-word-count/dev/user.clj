(ns user
  "Use this namespace for interactive development.

  This namespace requires libs needed to reset the app and helpers
  from `jackdaw.repl`. WARNING: Do no use `clj-refactor` (or
  equivalent) to clean this namespace since these tools cannot tell
  which libs are actually required."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [integrant.core :as ig]
            [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [jackdaw.admin :as ja]
            [jackdaw.serdes :as js]
            [jackdaw.repl :refer :all]
            [jackdaw.streams :as j]
            [jackdaw.streams.xform :as jxf]
            [xf-word-count :as xfwc]))


(integrant.repl/set-prep! (constantly repl-config))


(def repl-config
  "The development config.
  When the 'dev' alias is active, this config will be used."
  {:topology {:topology-builder xfwc/topology-builder
              :xform xfwc/xf
              :swap-fn jxf/kv-store-swap-fn}

   :topics {:streams-config xfwc/streams-config
            :client-config (select-keys xfwc/streams-config
                                        ["bootstrap.servers"])
            :topology (ig/ref :topology)}

   :app {:streams-config xfwc/streams-config
         :topology (ig/ref :topology)
         :topics (ig/ref :topics)}})


(defmethod ig/init-key :topology [_ {:keys [topology-builder xform swap-fn]}]
  (let [streams-builder (j/streams-builder)]
    ((topology-builder topic-metadata #(xform % swap-fn)) streams-builder)))

(defmethod ig/init-key :topics [_ {:keys [streams-config client-config topology]
                                   :as opts}]
  (let [topic-metadata (topology->topic-metadata topology streams-config)]
    (with-open [client (ja/->AdminClient client-config)]
      (ja/create-topics! client (vals topic-metadata)))
    (assoc opts :topic-metadata topic-metadata)))

(defmethod ig/init-key :app [_ {:keys [streams-config topology] :as opts}]
  (let [streams-app (j/kafka-streams topology streams-config)]
    (j/start streams-app)
    (assoc opts :streams-app streams-app)))

(defmethod ig/halt-key! :topics [_ {:keys [client-config topic-metadata]}]
  (let [re (re-pattern (str "(" (->> topic-metadata
                                     keys
                                     (map name)
                                     (str/join "|"))
                            ")"))]
    (re-delete-topics client-config re)))

(defmethod ig/halt-key! :app [_ {:keys [streams-config topics streams-app]}]
  (j/close streams-app)
  (destroy-state-stores streams-config)
  (let [re (re-pattern (str "(" (get streams-config "application.id") ")"))]
    (re-delete-topics (:client-config topics) re)))
