(defproject uswitch/bigquery "1.0.0"
  :description "Google Cloud BigQuery"
  :url "https://github.com/uswitch/gclouj"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.google.cloud/google-cloud-bigquery "2.1.7"]
                 [com.google.cloud/google-cloud-bigquerystorage "2.4.2"]
                 [org.apache.arrow/arrow-vector "6.0.0"]
                 [org.apache.arrow/arrow-memory-netty "6.0.0"]
                 [org.clojure/core.async "1.3.618"]
                 [org.clojure/tools.logging "1.1.0"]
                 [clj-time "0.15.2"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[org.apache.logging.log4j/log4j-core "2.9.0"]
                                  [org.apache.logging.log4j/log4j-slf4j-impl "2.9.0"]]
                   :jvm-opts ["-Dclojure.core.async.go-checking=true"]}})
