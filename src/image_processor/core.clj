(ns image-processor.core
  (:require [image-processor.worker :as worker])
  (:gen-class))

(defn -main
  "start a worker with 5 threads"
  [& args]
  (worker/work 5))
