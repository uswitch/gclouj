(ns gclouj.bigquery-storage
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

(defn- make-arrow-reader
  "TODO"
  [schema]
  (StorageRowReader. schema))

(defn- make-table-options [columns restrictions]
  (let [builder (ReadSession$TableReadOptions/newBuilder)]
    (when columns
      (.addAllSelectedFields builder columns))
    (when restrictions
      (.setRowRestriction builder restrictions))
    (.build builder)))

(defn read-data
  "TODO"
  ([billing-project data-project dataset table {:keys [columns restrictions] :as opts}]
   (with-open [client (open-client)]
     (read-data client billing-project data-project dataset table opts)))

  ([client billing-project data-project dataset table {:keys [columns restrictions]}]
   (let [src-table (format "projects/%s/datasets/%s/tables/%s" data-project dataset table)
         table-options (make-table-options columns restrictions)
         session-builder (.. ReadSession
                             (newBuilder)
                             (setTable src-table)
                             (setDataFormat DataFormat/ARROW)
                             (setReadOptions table-options))
         read-session-req (.. CreateReadSessionRequest
                              (newBuilder)
                              (setParent (format "projects/%s" billing-project))
                              (setReadSession session-builder)
                              (setMaxStreamCount 1)
                              (build))
         session (.createReadSession client read-session-req)]
     (with-open [reader (make-arrow-reader (.getArrowSchema session))]
       (when (<= (.getStreamsCount session) 0)
         (throw (ex-info "failed to read from BigQuery" {})))
       (let [stream-name (.. session (getStreams 0) (getName))
             read-rows-request (.. ReadRowsRequest
                                   (newBuilder)
                                   (setReadStream stream-name)
                                   (build))
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

  (def client (open-client))

  (read-data
    client
    "amp-compute"
    "uswitch-ldn"
    "dbt_gold"
    "clicks"
    {:restrictions "customer_full_ref = 'money/equity-release' AND DATE(click_timestamp) = '2021-10-27'"})

  (close-client client)
  )


