(ns jackdaw.serdes.fressian-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.data.fressian :as fressian]
            [jackdaw.serdes.fressian :as jsf])
  (:import java.net.URI
           [org.fressian.handlers WriteHandler ReadHandler]))

(set! *warn-on-reflection* false)

(defspec fressian-roundtrip-test 20
  (testing "Fressian data is the same after serialization and deserialization."
    (prop/for-all [x (gen/fmap #(apply str %) (gen/vector gen/char-alpha 100))]
                  (is (= x (->> (.serialize (jsf/fressian-serializer) nil x)
                                (.deserialize (jsf/fressian-deserializer) nil)))))))

(defspec fressian-reverse-roundtrip-test 20
  (testing "Fressian data is the same after deserialization and serialization."
    (prop/for-all [x (gen/fmap #(apply str %) (gen/vector gen/char-alpha 100))]
                  (let [bytes (.serialize (jsf/fressian-serializer) nil x)]
                    (is (= (seq bytes)
                           (seq (->> (.deserialize (jsf/fressian-deserializer) nil bytes)
                                     (.serialize (jsf/fressian-serializer) nil)))))))))

(def uri-tag "jackdaw/uri")

(def write-handlers
  (-> (merge {URI {uri-tag (reify WriteHandler
                             (write [_ writer uri]
                               (.writeTag writer uri-tag 1)
                               (.writeString writer (str uri))))}}
             fressian/clojure-write-handlers)
      fressian/associative-lookup
      fressian/inheritance-lookup))

(def read-handlers
  (-> (merge {uri-tag (reify ReadHandler
                        (read [_ reader tag component-count]
                          (URI. (.readObject reader))))}
             fressian/clojure-read-handlers)
      fressian/associative-lookup))

(defspec fressian-roundtrip-custom-handlers 20
  (testing "custom Fressian handlers"
    (prop/for-all [x (s/gen uri?)]
                  (is (= x (->> (.serialize (jsf/fressian-serializer :handlers write-handlers) nil x)
                                (.deserialize (jsf/fressian-deserializer :handlers read-handlers) nil)))))))
