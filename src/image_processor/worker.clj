(ns image-processor.worker
  (:import [java.awt.image BufferedImage]
           [java.awt.image BufferedImageOp]
           [java.io ByteArrayInputStream]
           [java.io ByteArrayOutputStream]
           [java.net URL]
           [org.imgscalr Scalr]
           [javax.imageio ImageIO]
           [java.util.concurrent LinkedBlockingQueue]
           [java.util UUID])
  (:require [clojure.java.io :as io]
            [aws.sdk.s3 :as s3]))

(declare pop-from-queue fetch resize upload)

(def bucket "bucket-name")

(def creds
  "aws access credentials"
  {:access-key "access key"
   :secret-key "secret token"})

(def queue
  "an in-memory, blocking work queue"
  (LinkedBlockingQueue.))

(defn image->input-stream
  "convert a buffered image to an input stream"
  [^BufferedImage image]
  (let [out (ByteArrayOutputStream.)]
    (ImageIO/write image "jpeg" out)
    (ByteArrayInputStream. (.toByteArray out))))

(defn run
  "run the given function on the given agent"
  [a f]
  (let [blocking? (:blocking (meta f))]
    (if blocking?
      (send a f)
      (send-off a f))))

(defn pop-from-queue
  [_]
  "pop a url from the queue"
  (try
    (.take queue)
    (finally
      (run *agent* #'fetch))))

(defn fetch
  "fetch an image from an s3 url"
  [^URL url]
  (try
    (ImageIO/read url)
    (finally
      (run *agent* #'resize))))

(defn ^:blocking resize
  "resize an image"
  [^BufferedImage img]
  (try
    (Scalr/resize img 750 0 (into-array BufferedImageOp []))
    (finally
      (run *agent* #'upload))))

(defn upload
  "upload a resized image to s3"
  [^BufferedImage img]
  (try
    (s3/put-object creds
                   bucket
                   (str "processed/" (UUID/randomUUID) ".jpeg")
                   (image->input-stream img)
                   {:cache-control "max-age=60"
                    :content-type "image/jpeg"}
                   (s3/grant :all-users :read))
    (finally
      (run *agent* #'pop-from-queue))))

(defn work
  "work from the in-memory queue"
  [n]
  (let [agents (repeatedly n #(agent nil))]
    (dorun (for [a agents] (run a #'pop-from-queue)))
    agents))
