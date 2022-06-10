(ns jackdaw.data
  "This namespace aims to provide `->T`, `(datafy T)`, and `data->T` as
  a round-tripping of Katka's (client) record types.

  Note that for some types, particularly Kafka's `-Result` types no
  `->T` constructors are provided as there are no consumers within the
  Kafka API for these records they are merely packed results.

  For compatibility with Clojure before 1.10.0, a `datafy` function is
  provided. On 1.10 or after, it simply defers to
  `clojure.datafy/datafy` but before 1.10 it acts as a backport
  thereof.

  "
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.walk :refer [stringify-keys keywordize-keys]])
  (:import java.util.Properties))

(set! *warn-on-reflection* true)

;;;; Datafy backport for pre 1.10

(try
  (require '[clojure.core.protocols :refer [Datafiable]])
  (catch java.lang.IllegalAccessError _
    (binding [*ns* (the-ns 'clojure.core.protocols)]
      (eval '(defprotocol Datafiable
               (datafy [o]))))
    (require '[clojure.core.protocols :refer [Datafiable]])
    (eval '(extend-protocol clojure.core.protocols/Datafiable
             Object
             (datafy [o] o)
             nil
             (datafy [o] o)))))

;;; Just vendor this - not worth the footwork to import the "real" one
;; Ignore clj-kondo's warning: Unresolved namespace clojure.core.protocols. Are you missing a require?
#_{:clj-kondo/ignore [:unresolved-namespace]}
(defn datafy
  "Attempts to return x as data.

  `datafy` will return the value of `#'clojure.core.protocols/datafy`.

  If the value has been transformed and the result supports metadata,
  `:clojure.datafy/obj` will be set on the metadata to the original
  value of x, and `:clojure.datafy/class` to the name of the class of
  x, as a symbol."
  [x]
  (let [v (clojure.core.protocols/datafy x)]
    (if (identical? v x)
      v
      (if (instance? clojure.lang.IObj v)
        (vary-meta v assoc
                   :clojure.datafy/obj x
                   :clojure.datafy/class (-> x class .getName symbol))
        v))))

;; Helper macro for the fact that every T->data function is really
;; intended to be a datafy handler. Not intended for general use, just
;; saves me the trouble of having to remember what I have and haven't
;; written a Datafiable entry for.

(defmacro ^:private defn->data [name & body]
  (let [tag (:tag (meta (ffirst (filter vector? body))))]
    (assert tag "Must be hinted!")
    `(do (defn ~name ~@body)
         (extend-protocol Datafiable
           ~tag (datafy [o#] (~name o#))))))

;;;; Properties

(defn map->Properties
  "Given a mapping of keywords to string values, stringify the keys via
  `#'clojure.walk/stringify-keys` and return a `Properties` object
  with the transformed keys and unmodified values."
  ^Properties [m]
  (let [props (Properties.)]
    (when m
      (.putAll props (stringify-keys m)))
    props))

(defn->data Properties->data
  "Consume a `Properties` instance, keywordizing the keys and returning
  a Clojure mapping of the resulting keys to unmodified values."
  [^Properties o]
  (keywordize-keys (into {} o)))

(load "/jackdaw/data/common")
(load "/jackdaw/data/common_config")
(load "/jackdaw/data/common_record")
(load "/jackdaw/data/admin")
(load "/jackdaw/data/consumer")
(load "/jackdaw/data/producer")
