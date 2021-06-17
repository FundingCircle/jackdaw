(ns roll-dice.core
  "This sample app is a simple Kafka application to demonstrate how to use
  Jackdaw's Client API (Producer and Consumer).

  It rolls a dice `n` number of times (input by the user).
  A Jackdaw Producer then writes the numbers to the input topic `rolldice`.
  A Jackdaw Consumer reads from the topic and adds up the numbers
  and prints out the result."
  (:require
   [roll-dice.kafka :as kafka]))

(defn roll-dice [n]
  (repeatedly n
    #(let [rnd-side  (fn [] (inc (rand-int 6)))]
      [(rnd-side) (rnd-side)])))

(defn process-records [records]
  (let [numbers (:value (first records))
        sums    (mapv #(apply + %) numbers)]
    (println "numbers: " numbers)
    (println "sums: " sums)))

(defn start-consumer-thread! [topic group-id]
  (-> (Thread. #(kafka/process-messages! topic group-id process-records))
      (.start)))

(defn -main []
  (let [topic    "rolldice"
        group-id "rolldice-consumer"]
    (start-consumer-thread! topic group-id)
    (loop []
      (println "How many times would you like to roll the dice? \nPlease enter a positive integer:")
      (let [n (Integer/parseInt (read-line))
            roll-n-times! #(roll-dice n)]
        (kafka/produce-message! topic roll-n-times!)
        (Thread/sleep 2000)
        (println "\nPress Ctr+c to exit or keep rolling.\n")
        (recur)))))
