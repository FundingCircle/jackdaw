(ns jackdaw.test.transports)

(defmulti transport (fn [config]
                      (:type config)))

(defmethod transport :default
  [cfg]
  (throw (ex-info "unable to find transport to satisfy config" {})))

(defn with-transport
  [machine transport]
  (let [machine' (merge-with concat
                             (dissoc machine :transport)
                             transport)]
    (when-not (get-in machine' [:consumer :messages])
      (throw (ex-info "no messages channel provided by selected transport"
                      {})))
    machine'))

