(defproject fundingcircle/jackdaw-extras "0.3.26"
  :description "Extra tools for developing with kafka that draw from all the other jackdaw utilities"

  :plugins [[fundingcircle/lein-modules "[0.3.0,0.4.0)"]]

  :dependencies [[fundingcircle/jackdaw-admin "_"]
                 [fundingcircle/jackdaw-client "_"]
                 [fundingcircle/jackdaw-serdes "_"]
                 [fundingcircle/jackdaw-test "_"]
                 [org.clojure/data.json "_"]])
