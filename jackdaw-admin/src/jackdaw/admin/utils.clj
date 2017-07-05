(ns jackdaw.admin.utils)

(defn stringify-keys [m]
  (apply hash-map (mapcat (fn [[k v]]
                            [(name k) v]) m)))

(defn map->properties
  [m]
  (doto (java.util.Properties.)
    (.putAll (stringify-keys m))))
