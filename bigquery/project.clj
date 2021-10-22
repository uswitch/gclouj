(defproject uswitch/bigquery "1.0.0"
  :description "Google Cloud BigQuery"
  :url "https://github.com/uswitch/gclouj"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.google.cloud/google-cloud-bigquery "2.1.7"]
                 [com.google.cloud/google-cloud-bigquerystorage "2.4.2"]
                 [clj-time "0.15.2"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}}})