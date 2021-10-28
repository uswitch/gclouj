(ns gclouj.bigquery-storage
  (:require [clojure.core.async :as a])
  (:import (com.google.cloud.bigquery.storage.v1
             BigQueryReadClient
             ReadSession$TableReadOptions
             ReadSession
             DataFormat
             CreateReadSessionRequest
             ReadRowsRequest
             ReadRowsResponse)
           (gclouj StorageRowReader CallbackStorageRowReader)
           (clojure.lang ISeq Associative)
           (org.apache.arrow.vector.util Text)
           (java.util List Map)))

(defprotocol ToClojure
  (to-clojure [_]))

(extend-protocol ToClojure
  nil (to-clojure [x] x)
  String (to-clojure [x] x)
  Number (to-clojure [x] x)
  Boolean (to-clojure [x] (.booleanValue x))
  ISeq (to-clojure [x] (map to-clojure x))
  List (to-clojure [x] (map to-clojure x))
  Associative (to-clojure [x] (->> x
                                   (map (fn [[k v]]
                                          [(to-clojure k) (to-clojure v)]))
                                   (into {})))
  Map (to-clojure [x] (->> x
                           (map (fn [[k v]]
                                  [(to-clojure k) (to-clojure v)]))
                           (into {})))
  Text (to-clojure [^Text x] (.toString x)))

(defn open-client
  "TODO"
  []
  (BigQueryReadClient/create))

(defn close-client
  [client]
  (.close client))

(defn- make-sync-arrow-reader
  "TODO"
  [schema]
  (StorageRowReader. schema))

(defn- make-async-arrow-reader
  "TODO"
  [schema]
  (CallbackStorageRowReader. schema))

(defn- make-table-options [columns restrictions]
  (let [builder (ReadSession$TableReadOptions/newBuilder)]
    (when columns
      (.addAllSelectedFields builder columns))
    (when restrictions
      (.setRowRestriction builder restrictions))
    (.build builder)))

(defn- make-session-builder
  "TODO"
  [src-table table-options]
  (.. ReadSession
      (newBuilder)
      (setTable src-table)
      (setDataFormat DataFormat/ARROW)
      (setReadOptions table-options)))

(defn make-session-request
  "TODO"
  [billing-project session-builder max-stream-count]
  (.. CreateReadSessionRequest
      (newBuilder)
      (setParent (format "projects/%s" billing-project))
      (setReadSession session-builder)
      (setMaxStreamCount max-stream-count)
      (build)))

(defn make-read-rows-request
  "TODO"
  [stream-name]
  (.. ReadRowsRequest
      (newBuilder)
      (setReadStream stream-name)
      (build)))

(def per-stream-channel-buffer 50)
(def full-channel-buffer 1000)

(defn read-stream-async
  "TODO"
  [client reader stream]
  (let [result-chan (a/chan per-stream-channel-buffer (map to-clojure))
        stream-name (.getName stream)
        read-rows-request (make-read-rows-request stream-name)
        batches (.. client (readRowsCallable) (call read-rows-request))]
    (a/go
      (for [batch batches]
        (do
          (when (not (.hasArrowRecordBatch batch))
            (throw (ex-info "funny state detekt" {})))
          (let [rows (.getArrowRecordBatch batch)]
            (.processRows reader rows
                          (partial a/>! result-chan)))
          (a/close! result-chan)))
      result-chan)))

(defn read-data-async
  "TODO"
  [client billing-project data-project dataset table {:keys [columns restrictions]}]
  (let [src-table (format "projects/%s/datasets/%s/tables/%s" data-project dataset table)
        table-options (make-table-options columns restrictions)
        session-builder (make-session-builder src-table table-options)
        read-session-req (make-session-request billing-project session-builder 0)
        session (.createReadSession client read-session-req)]

    (with-open [reader (make-async-arrow-reader (.getArrowSchema session))]

      (when (<= (.getStreamsCount session) 0)
        (throw (ex-info "failed to read from BigQuery" {})))

      (let [streams (.getStreamsList session)
            stream-chans (map
                           (partial read-stream-async client reader)
                           streams)]
        (a/merge stream-chans full-channel-buffer)))))

(defn read-data
  "TODO"
  ([billing-project data-project dataset table {:keys [columns restrictions] :as opts}]
   (with-open [client (open-client)]
     (read-data client billing-project data-project dataset table opts)))

  ([client billing-project data-project dataset table {:keys [columns restrictions]}]
   (let [src-table (format "projects/%s/datasets/%s/tables/%s" data-project dataset table)
         table-options (make-table-options columns restrictions)
         session-builder (make-session-builder src-table table-options)
         read-session-req (make-session-request billing-project session-builder 1)
         session (.createReadSession client read-session-req)]

     (with-open [reader (make-sync-arrow-reader (.getArrowSchema session))]
       (when (<= (.getStreamsCount session) 0)
         (throw (ex-info "failed to read from BigQuery" {})))
       (let [stream-name (.. session (getStreams 0) (getName))
             read-rows-request (make-read-rows-request stream-name)
             stream (.. client (readRowsCallable) (call read-rows-request))]
         (mapcat (fn [^ReadRowsResponse batch]
                   (when (not (.hasArrowRecordBatch batch))
                     (throw (ex-info "funny state detekt" {})))
                   (let [rows (.getArrowRecordBatch batch)]
                     (to-clojure (.processRows reader rows))))
                 stream))))
   ))

(comment
  ; TODO check actual correctness of all this data
  (read-data
    "amp-compute"
    "uswitch-ldn"
    "dbt_gold"
    "clicks"
    {:restrictions "customer_full_ref = 'money/equity-release' AND DATE(click_timestamp) = '2021-10-27'"})


  (read-data
    client
    "amp-compute"
    "uswitch-ldn"
    "dbt_gold"
    "clicks"
    {:restrictions "customer_full_ref = 'money/equity-release' AND DATE(click_timestamp) = '2021-10-27'"})

  (close-client client)

  (def client (open-client))

  (a/into [] (read-data-async
               client
               "amp-compute"
               "uswitch-ldn"
               "dbt_gold"
               "clicks"
               {:restrictions "customer_full_ref = 'money/equity-release' AND DATE(click_timestamp) = '2021-10-27'"}))

  (def results (read-data-async
                 client
                 "amp-compute"
                 "uswitch-ldn"
                 "dbt_gold"
                 "clicks"
                 {:restrictions "customer_full_ref = 'money/equity-release' AND DATE(click_timestamp) = '2021-10-27'"}))

  (a/go-loop
    []
    (let [results-chan (read-data-async
                         client
                         "amp-compute"
                         "uswitch-ldn"
                         "dbt_gold"
                         "clicks"
                         {:restrictions "customer_full_ref = 'money/equity-release' AND DATE(click_timestamp) = '2021-10-27'"})]
      (a/into [] results-chan)))

  )


