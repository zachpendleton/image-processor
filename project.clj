(defproject image-processor "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.imgscalr/imgscalr-lib "4.2"]
                 [clj-aws-s3 "0.3.8"]]
  :main ^:skip-aot image-processor.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
