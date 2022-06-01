(ns jackdaw.specs-test
  (:require [jackdaw.specs :refer [exactly-one-true?]]
            [clojure.test :refer [deftest are]]))

(deftest exactly-one-true?-test
  (are [x y] (= x (apply exactly-one-true? y))
       true [false true false]
       false [true true false]
       false [false false false false false]
       true [nil 1 nil nil false]))
