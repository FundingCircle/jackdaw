(ns kafka.test.fs
  (:require
   [clojure.java.io :as io]))

(defn tmp-dir
  "Returns the name of a temporary directory

   Use `parts` to obtain the corresponding sub-directory of
   the root temporary dir."
  [& parts]
  (let [gen-dir (fn [root]
                  (apply io/file root "kafka.test" parts))]
    (-> (System/getProperty "java.io.tmpdir")
        (gen-dir)
        (.getPath))))

(defn try-delete!
  "Recursively delete everything under `dir` and then delete `dir`"
  [dir]
  (try
    (doseq [f (file-seq dir)]
      (.delete f))
    (.delete dir)
    (catch Exception e
      (println "failed to delete logs: " e))))
