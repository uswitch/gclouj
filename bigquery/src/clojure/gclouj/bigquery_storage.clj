(ns gclouj.bigquery-storage
  "Tools for querying BigQuery via the Storage API.

  https://cloud.google.com/bigquery/docs/reference/storage

  This is usually faster, cheaper, and allows for streaming.

  The main restriction is only 1 table allowed and it has to be a physical table, because the
  API reads the underlying storage directly."
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log])
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
           (java.util List Map)
           (java.util.function Consumer)))

(def per-stream-channel-buffer 500)
(def full-channel-buffer 500)

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
  "Constructs a Storage API client."
  []
  (BigQueryReadClient/create))

(defn close-client
  "Closes a given Storage API client."
  [client]
  (.close client))

(defn- make-sync-arrow-reader
  [schema]
  (StorageRowReader. schema))

(defn- make-async-arrow-reader
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
  [src-table table-options]
  (.. ReadSession
      (newBuilder)
      (setTable src-table)
      (setDataFormat DataFormat/ARROW)
      (setReadOptions table-options)))

(defn- make-session-request
  [billing-project session-builder max-stream-count]
  (.. CreateReadSessionRequest
      (newBuilder)
      (setParent (format "projects/%s" billing-project))
      (setReadSession session-builder)
      (setMaxStreamCount max-stream-count)
      (build)))

(defn- make-read-rows-request
  [stream-name]
  (.. ReadRowsRequest
      (newBuilder)
      (setReadStream stream-name)
      (build)))

(defn- read-stream-async
  [client schema stream]
  (let [stream-name (.getName stream)
        read-rows-request (make-read-rows-request stream-name)
        batches (.. client (readRowsCallable) (call read-rows-request))]
    (with-open [reader (make-sync-arrow-reader schema)]
      (let [batch-chans (map
                          (fn [batch]
                            (do
                              (when (not (.hasArrowRecordBatch batch))
                                (throw (ex-info "funny state detekt" {})))
                              (let [rows (.getArrowRecordBatch batch)
                                    batch-rows (.processRows reader rows)]
                                (log/debugf "Returning %s items from %s" (count batch-rows) stream-name)
                                (a/to-chan! batch-rows))))
                          batches)]
        (a/merge batch-chans per-stream-channel-buffer)))))

(defn read-data-async
  "Reads data from Bigquery Storage API asynchronously.

  client: a Storage API client. Ensuring the client survives beyond all data being written out to the
          result channel is the responsibility of the user.
  billing-project: which project to bill your query against
  data-project: project where data is located
  dataset: dataset to query
  table: table name
  opts: hashmap of options
    {
      :columns - selector for returning only a subset of columns, default 'nil' for all columns
      :restrictions - string for restrictions, in WHERE-clause syntax
    }

  Return is a core.async channel that will have hashmaps of your data. The resulting channel
  will be closed when all your data is returned.
  "
  [client billing-project data-project dataset table {:keys [columns restrictions]}]
  (let [src-table (format "projects/%s/datasets/%s/tables/%s" data-project dataset table)
        table-options (make-table-options columns restrictions)
        session-builder (make-session-builder src-table table-options)
        read-session-req (make-session-request billing-project session-builder 0)
        session (.createReadSession client read-session-req)
        schema (.getArrowSchema session)
        stream-count (.getStreamsCount session)]

    (when (<= stream-count 0)
      (throw (ex-info "failed to read from BigQuery" {})))

    (log/infof "Parsing %s streams of data" stream-count)
    (let [streams (.getStreamsList session)
          stream-chans (map
                         (partial read-stream-async client schema)
                         streams)]
      (a/map to-clojure [(a/merge stream-chans full-channel-buffer)]))))

(defn read-data
  "Reads data from Bigquery Storage API synchronously.

  client: a Storage API client. The overload that doesn't require it will make a client for you,
          but for repeat querying it's advisable you construct and reuse one in your application.
  billing-project: which project to bill your query against
  data-project: project where data is located
  dataset: dataset to query
  table: table name
  opts: hashmap of options
    {
      :columns - selector for returning only a subset of columns, default 'nil' for all columns
      :restrictions - string for restrictions, in WHERE-clause syntax
    }

  Return is a vector of hashmaps with your data.
  "
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
                 stream))))))

(comment
  (close-client client)

  (def client (open-client))

  (def data (read-data
              client
              "amp-compute"
              "uswitch-ldn"
              "dbt_gold"
              "clicks"
              {:restrictions "customer_full_ref = 'money/equity-release' AND DATE(click_timestamp) = '2021-10-27'"}))

  (first data)

  (def re-cha (read-data-async
                client
                "amp-compute"
                "uswitch-ldn"
                "dbt_gold"
                "clicks"
                {:restrictions "customer_full_ref = 'money/unsecured-loans' AND DATE(click_timestamp) >= '2021-10-27'"}))
  (count
    (a/<!!
      (a/into
        []
        re-cha
        )))

  (def f1 (a/<!! re-cha))

  (a/<!! first-return)

  )
