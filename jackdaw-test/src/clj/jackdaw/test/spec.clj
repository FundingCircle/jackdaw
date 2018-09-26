(ns jackdaw.test.spec
  (:require [clojure.spec.gen.alpha :as gen]
            [clojure.spec.alpha :as s]))


(defmacro generate-> [spec & body]
  (let [data-form (reduce (fn [acc [f & rest]]
                            `(~f ~acc ~@rest))
                          `(gen/generate (s/gen ~spec))
                          body)]
    `(let [x# ~data-form]
       (when-not (s/valid? ~spec x#)
         (throw (ex-info "Invalid data assigned!" (s/explain-data ~spec x#))))
       x#)))
