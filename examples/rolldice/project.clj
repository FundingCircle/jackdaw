(defproject rolldice "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [fundingcircle/jackdaw "0.7.8"]
                 [proto-repl "0.3.1"]
                 [pjstadig/humane-test-output "0.11.0"]
                 [com.taoensso/timbre "4.2.0"]
                 [org.slf4j/slf4j-nop "1.7.30"]]
  :main ^:skip-aot roll-dice.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
             :dev {:plugins [[com.jakemccrary/lein-test-refresh "0.24.1"]]}})
