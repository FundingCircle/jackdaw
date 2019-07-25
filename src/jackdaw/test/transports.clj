(ns jackdaw.test.transports)

(defonce +transports+ (atom #{}))

(defmulti transport :type)

(defn supported-transports []
  @+transports+)

(defmethod transport :default
  [_cfg]
  (throw (ex-info "unable to find transport to satisfy config" {})))

(defmacro deftransport [transport-type args & body]
  `(do
     (defmethod transport ~transport-type
       ~args
       ~@body)
     (swap! +transports+ conj ~transport-type)))

(defn with-transport
  [machine transport]
  (let [machine' (merge-with concat
                             (dissoc machine :transport)
                             transport)]
    (when-not (get-in machine' [:consumer :messages])
      (throw (ex-info "no messages channel provided by selected transport"
                      {})))
    machine'))
