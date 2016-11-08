(ns kafka.test.fs
  (:require
   [clojure.java.io :as io]))

(defn tmp-dir-exists? [dir]
  (.exists (io/as-file dir)))

(defn tmp-dir
  "Returns the name of a temporary directory

   Use `parts` to obtain the corresponding sub-directory of
   the root temporary dir."
  [& parts]
  (let [gen-dir (fn [root]
                  (apply io/file root "embedded-kafka" parts))]
    (-> (System/getProperty "java.io.tmpdir")
        (gen-dir)
        (.getPath))))

(defn delete-directory [d]
  "Delete directory recursively

   If `d` is a file, just delete it. If it is a directory, first delete
   all children then delete `d`."
  (let [f (io/file d)]
    (when (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-directory child)))
    (io/delete-file f)))

(defn try-delete!
  "Recursively delete everything under `dir` and then delete `dir`"
  [dir]
  (try
    (delete-directory dir)
    (catch Exception e
      (throw (ex-info "Failed to cleanup test state due to exception"
                      {:dir dir
                       :exception e})))))
