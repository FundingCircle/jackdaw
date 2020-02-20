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
            [simple-ledger :as sl]))


(def repl-config
  "The development config.
  When the 'dev' alias is active, this config will be used."
  {:topics {:client-config (select-keys sl/streams-config ["bootstrap.servers"])
            :topic-metadata {:entry-pending
                             {:topic-name "entry-pending"
                              :partition-count 15
                              :replication-factor 1
                              :key-serde (js/edn-serde)
                              :value-serde (js/edn-serde)}

                             :transaction-pending
                             {:topic-name "transaction-pending"
                              :partition-count 15
                              :replication-factor 1
                              :key-serde (js/edn-serde)
                              :value-serde (js/edn-serde)}

                             :transaction-added
                             {:topic-name "transaction-added"
                              :partition-count 15
                              :replication-factor 1
                              :key-serde (js/edn-serde)
                              :value-serde (js/edn-serde)}}}

   :topology {:topology-builder sl/topology-builder
              :xforms [#'sl/split-entries #'sl/running-balances]
              :swap-fn jxf/kv-store-swap-fn}

   :app {:streams-config sl/streams-config
         :topology (ig/ref :topology)
         :topics (ig/ref :topics)}})


(integrant.repl/set-prep! (constantly repl-config))


(defmethod ig/init-key :topics [_ {:keys [client-config topic-metadata] :as opts}]
  (with-open [client (ja/->AdminClient client-config)]
    (ja/create-topics! client (vals topic-metadata)))
  (assoc opts :topic-metadata topic-metadata))

(defmethod ig/init-key :topology [_ {:keys [topology-builder xforms swap-fn]}]
  (let [xform-map (reduce-kv (fn [m k v]
                               (let [k (keyword (str (:ns (meta v)))
                                                (str (:name (meta v))))]
                                 (assoc m k #(v % jxf/kv-store-swap-fn))))
                               {}
                               xforms)
        streams-builder (j/streams-builder)]
    ((topology-builder topic-metadata xform-map) streams-builder)))

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
