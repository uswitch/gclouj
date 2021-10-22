(ns gclouj.bigquery
  (:require [clojure.walk :as walk]
            [clj-time.coerce :as tc])
  (:import [com.google.cloud.bigquery
            BigQueryOptions
            BigQuery$DatasetListOption
            DatasetInfo
            DatasetId
            BigQuery$TableListOption
            StandardTableDefinition
            TableId
            BigQuery$DatasetOption
            BigQuery$TableOption
            Schema
            Field
            Field$Mode
            StandardTableDefinition$StreamingBuffer
            InsertAllRequest
            InsertAllRequest$RowToInsert
            InsertAllResponse
            BigQueryError
            BigQuery$DatasetDeleteOption
            JobId
            Field
            FieldValue
            FieldValue$Attribute
            BigQuery$JobOption
            JobInfo$CreateDisposition
            JobInfo$WriteDisposition
            JobStatistics
            JobStatistics$LoadStatistics
            JobStatus
            JobStatus$State
            FormatOptions
            UserDefinedFunction
            JobInfo
            ExtractJobConfiguration
            LoadJobConfiguration
            QueryJobConfiguration
            QueryJobConfiguration$Priority
            Table
            BigQuery$QueryResultsOption
            TableInfo
            ViewDefinition
            CopyJobConfiguration
            LegacySQLTypeName
            FieldList StandardSQLTypeName BigQuery TableResult]
           [gclouj BigQueryOptionsFactory]
           [com.google.common.hash Hashing]
           [java.util List Collections]
           [java.util.concurrent TimeUnit]))


(defmulti field-value->clojure (fn [attribute val]
                                 attribute))
(defmethod field-value->clojure FieldValue$Attribute/PRIMITIVE [_ ^FieldValue val]
  (.getValue val))

(defn- parse-timestamp [val]
  (let [seconds (long (Double/valueOf val))
        millis (.toMillis (TimeUnit/SECONDS) seconds)]
    (tc/from-long millis)))

(def cell-coercions {:integer   #(when % (Long/valueOf %))
                     :bool      #(when % (Boolean/valueOf %))
                     :float     #(when % (Double/valueOf %))
                     :string    identity
                     :timestamp #(when % (parse-timestamp %))})

(defn- coerce-result
  [schema]
  (let [coercions (map cell-coercions (map :type schema))
        names (map :name schema)]
    (fn [row]
      (->> (for [[name coerce value] (partition 3 (interleave names coercions row))]
             [name (coerce value)])
           (into {})))))

(defprotocol ToClojure
  (to-clojure [_]))

(extend-protocol ToClojure
  DatasetId
  (to-clojure [x] {:dataset-id (.getDataset x)
                   :project-id (.getProject x)})
  DatasetInfo
  (to-clojure [x] {:creation-time (.getCreationTime x)
                   :dataset-id    (to-clojure (.getDatasetId x))
                   :description   (.getDescription x)
                   :friendly-name (.getFriendlyName x)
                   :location      (.getLocation x)
                   :last-modified (.getLastModified x)})
  TableId
  (to-clojure [x] {:dataset-id (.getDataset x)
                   :project-id (.getProject x)
                   :table-id   (.getTable x)})
  StandardTableDefinition$StreamingBuffer
  (to-clojure [x] {:estimated-bytes   (.getEstimatedBytes x)
                   :estimated-rows    (.getEstimatedRows x)
                   :oldest-entry-time (.getOldestEntryTime x)})
  StandardTableDefinition
  (to-clojure [x] {:location         (.getLocation x)
                   :bytes            (.getNumBytes x)
                   :rows             (.getNumRows x)
                   :streaming-buffer (when-let [sb (.getStreamingBuffer x)] (to-clojure sb))
                   :schema           (when-let [schema (.getSchema x)] (to-clojure schema))})
  ViewDefinition
  (to-clojure [x] {:schema (when-let [schema (.getSchema x)]
                             (to-clojure schema))})
  Table
  (to-clojure [x] {:creation-time (.getCreationTime x)
                   :description   (.getDescription x)
                   :friendly-name (.getFriendlyName x)
                   :table-id      (to-clojure (.getTableId x))
                   :definition    (to-clojure (.getDefinition x))})
  BigQueryError
  (to-clojure [x] {:reason   (.getReason x)
                   :location (.getLocation x)
                   :message  (.getMessage x)})
  InsertAllResponse
  (to-clojure [x] {:errors (->> (.getInsertErrors x)
                                (map (fn [[idx errors]]
                                       {:index  idx
                                        :errors (map to-clojure errors)}))
                                (seq))})
  JobId
  (to-clojure [x] {:project-id (.getProject x)
                   :job-id     (.getJob x)})
  Field
  (to-clojure [x] (let [type (.getType x)]
                    {:name   (.getName x)
                     :mode   ({Field$Mode/NULLABLE :nullable
                               Field$Mode/REPEATED :repeated
                               Field$Mode/REQUIRED :required} (.getMode x))
                     :type   ({LegacySQLTypeName/BOOLEAN   :bool
                               LegacySQLTypeName/FLOAT     :float
                               LegacySQLTypeName/INTEGER   :integer
                               LegacySQLTypeName/RECORD    :record
                               LegacySQLTypeName/STRING    :string
                               LegacySQLTypeName/TIMESTAMP :timestamp} type)
                     :fields (when (= LegacySQLTypeName/RECORD type)
                               (map to-clojure (.getSubFields x)))}))
  FieldValue
  (to-clojure [x] (field-value->clojure (.getAttribute x) x))

  Schema
  (to-clojure [x] (map to-clojure (.getFields x)))

  TableResult
  (to-clojure [x]
    (let [schema (to-clojure (.getSchema x))]
      (->> (seq (.iterateAll x))
           (map (fn [fields] (map to-clojure fields)))
           (map (coerce-result schema)))))

  JobStatistics$LoadStatistics
  (to-clojure [x] {:input-bytes  (.getInputBytes x)
                   :input-files  (.getInputFiles x)
                   :output-bytes (.getOutputBytes x)
                   :output-rows  (.getOutputRows x)})
  JobStatistics
  (to-clojure [x] {:created (.getCreationTime x)
                   :end     (.getEndTime x)
                   :started (.getStartTime x)})
  JobStatus
  (to-clojure [x] {:state  ({JobStatus$State/DONE    :done
                             JobStatus$State/PENDING :pending
                             JobStatus$State/RUNNING :running} (.getState x))
                   :errors (seq (map to-clojure (.getExecutionErrors x)))})
  JobInfo
  (to-clojure [x] {:job-id     (to-clojure (.getJobId x))
                   :statistics (to-clojure (.getStatistics x))
                   :email      (.getUserEmail x)
                   :status     (to-clojure (.getStatus x))}))

(defn service
  ([] (.getService (BigQueryOptions/getDefaultInstance)))
  ([{:keys [project-id]}]
   (.getService (BigQueryOptionsFactory/create project-id))))

(defn datasets [^BigQuery service]
  (let [it (-> service
               (.listDatasets (into-array BigQuery$DatasetListOption []))
               (.iterateAll))]
    (map to-clojure (seq it))))

(defn dataset [^BigQuery service {:keys [project-id dataset-id]}]
  (when-let [dataset (.getDataset service
                                  ^DatasetId (DatasetId/of project-id dataset-id)
                                  #^BigQuery$DatasetOption (into-array BigQuery$DatasetOption []))]
    (to-clojure dataset)))

(defn tables
  "Returns a sequence of table-ids. For complete table
  information (schema, location, size etc.) you'll need to also use the
  `table` function."
  [^BigQuery service {:keys [project-id dataset-id]}]
  (let [it (-> service
               (.listTables (DatasetId/of project-id dataset-id)
                            (into-array BigQuery$TableListOption []))
               (.iterateAll))]
    (map to-clojure (seq it))))

(defn table [service {:keys [project-id dataset-id table-id] :as table}]
  (to-clojure (.getTable service
                         (TableId/of project-id dataset-id table-id)
                         (into-array BigQuery$TableOption []))))

(defn create-dataset [^BigQuery service
                      {:keys [project-id dataset-id friendly-name location description table-lifetime-millis]}]
  (let [locations {:eu "EU"
                   :us "US"}
        builder (DatasetInfo/newBuilder project-id dataset-id)]
    (when friendly-name
      (.setFriendlyName builder friendly-name))
    (when description
      (.setDescription builder description))
    (when table-lifetime-millis
      (.setDefaultTableLifetime builder table-lifetime-millis))
    (.setLocation builder (or (locations location) "US"))
    (to-clojure (.create service
                         ^DatasetInfo (.build builder)
                         #^BigQuery$DatasetOption (into-array BigQuery$DatasetOption [])))))

(defn delete-dataset [^BigQuery service {:keys [project-id dataset-id delete-contents?]}]
  (let [options (if delete-contents?
                  [(BigQuery$DatasetDeleteOption/deleteContents)]
                  [])]
    (.delete service
             ^DatasetId (DatasetId/of project-id dataset-id)
             #^BigQuery$DatasetDeleteOption  (into-array BigQuery$DatasetDeleteOption options))))

(defn- mkfield [{:keys [name type mode fields]}]
  (let [field-type (condp = type
                     :bool StandardSQLTypeName/BOOL
                     :float StandardSQLTypeName/FLOAT64
                     :integer StandardSQLTypeName/INT64
                     :string StandardSQLTypeName/STRING
                     :timestamp StandardSQLTypeName/TIMESTAMP
                     :record StandardSQLTypeName/STRUCT)
        builder (Field/newBuilder
                  ^String name
                  ^StandardSQLTypeName field-type
                  ^FieldList (FieldList/of (map mkfield fields)))
        field-mode ({:nullable (Field$Mode/NULLABLE)
                     :repeated (Field$Mode/REPEATED)
                     :required (Field$Mode/REQUIRED)} (or mode :nullable))]
    (.setMode builder field-mode)
    (.build builder)))

(defn- mkschema
  [fields]
  (Schema/of #^Field (into-array Field (->> fields (map mkfield)))))

(defn create-table
  "Fields: sequence of fields representing the table schema.
  e.g. [{:name \"foo\" :type :record :fields [{:name \"bar\" :type :integer}]}]"
  [^BigQuery service {:keys [project-id dataset-id table-id]} fields]
  (let [builder (TableInfo/newBuilder (TableId/of project-id dataset-id table-id)
                                      (StandardTableDefinition/of (mkschema fields)))
        table-info (.build builder)]
    (to-clojure (.create service table-info (into-array BigQuery$TableOption [])))))

(defn delete-table
  [^BigQuery service {:keys [project-id dataset-id table-id] :as table}]
  (.delete service (TableId/of project-id dataset-id table-id)))

(defn row-hash
  "Creates a hash suitable for identifying duplicate rows, useful when
  streaming to avoid inserting duplicate rows."
  [m & {:keys [bits] :or {bits 128}}]
  (-> (Hashing/goodFastHash bits) (.hashUnencodedChars (pr-str m)) (.toString)))

(defn- row-value [m]
  ;; the google client incorrectly interprets clojure maps as arrays so
  ;; we wrap in an unmodifiableMap to ensure the client interprets
  ;; correctly.
  (letfn [(wrap-map [x]
            (if (map? x)
              (Collections/unmodifiableMap x)
              x))]
    (walk/postwalk wrap-map m)))

(defn- insert-row [row-id row]
  (if row-id
    (InsertAllRequest$RowToInsert/of (row-id row) (row-value row))
    (InsertAllRequest$RowToInsert/of (row-value row))))
(defn insert-all
  "Performs a streaming insert of rows. row-id can be a function to
  return the unique identity of the row (e.g. row-hash). Template suffix
  can be used to create tables according to a template."
  [^BigQuery service {:keys [project-id dataset-id table-id skip-invalid? template-suffix row-id] :as table} rows]
  (let [builder (InsertAllRequest/newBuilder (TableId/of project-id dataset-id table-id)
                                             ^Iterable (map (partial insert-row row-id) rows))]
    (when template-suffix
      (.setTemplateSuffix builder template-suffix))
    (->> builder
         (.build)
         (.insertAll service)
         (to-clojure))))

(defn query
  "Executes a query. BigQuery will create a Query Job and block for the
  specified timeout. If the query returns within the time the results
  will be returned. Otherwise, results need to be retrieved separately
  using query-results. Status of the job can be checked using the job
  function, and checking completed?"
  [^BigQuery service
   ^String query
   {:keys [max-results dry-run? max-wait-millis use-cache? use-legacy-sql? default-dataset] :as query-opts}]
  (let [builder (QueryJobConfiguration/newBuilder query)]
    (when default-dataset
      (let [{:keys [project-id dataset-id]} default-dataset]
        (.setDefaultDataset builder (DatasetId/of project-id dataset-id))))
    (when max-results
      (.setMaxResults builder max-results))
    (when-not (nil? dry-run?)
      (.setDryRun builder dry-run?))
    (when max-wait-millis
      (.setJobTimeoutMs builder max-wait-millis))
    (when-not use-legacy-sql?
      (.setUseLegacySql builder use-legacy-sql?))
    (.setUseQueryCache builder use-cache?)
    (let [q (.build builder)]
      (to-clojure (.query service q (into-array BigQuery$JobOption []))))))

(defmulti query-option (fn [[type _]] type))

(defmethod query-option :max-wait-millis [[_ val]] (BigQuery$QueryResultsOption/maxWaitTime val))

(defn query-results
  "Retrieves results for a Query job. Will throw exceptions unless Job
  has completed successfully. Check using job and completed? functions."
  [^BigQuery service {:keys [project-id job-id]} & opts]
  {:pre [(string? job-id) (string? project-id)]}
  (to-clojure (.getQueryResults service
                                (JobId/of project-id job-id)
                                (->> opts (map query-option) (into-array BigQuery$QueryResultsOption)))))

(defn job [service {:keys [project-id job-id] :as job}]
  (to-clojure (.getJob service (JobId/of project-id job-id) (into-array BigQuery$JobOption []))))

(defn successful? [job]
  (and (= :done (get-in job [:status :state]))
       (empty? (get-in job [:status :errors]))))

(defn running? [job]
  (= :running (get-in job [:status :state])))

(def create-dispositions {:needed JobInfo$CreateDisposition/CREATE_IF_NEEDED
                          :never  JobInfo$CreateDisposition/CREATE_NEVER})

(def write-dispositions {:append   JobInfo$WriteDisposition/WRITE_APPEND
                         :empty    JobInfo$WriteDisposition/WRITE_EMPTY
                         :truncate JobInfo$WriteDisposition/WRITE_TRUNCATE})

(defn table-id [{:keys [project-id dataset-id table-id]}]
  (TableId/of project-id dataset-id table-id))

(defn execute-job [^BigQuery service job]
  (to-clojure (.create service
                       ^JobInfo (.build (JobInfo/newBuilder job))
                       #^BigQuery$JobOption (into-array BigQuery$JobOption []))))

(defn load-job
  "Loads data from Cloud Storage URIs into the specified table.
  Table argument needs to be a map with project-id, dataset-id and table-id.
  Options:
  `create-disposition` controls whether tables are created if
  necessary, or assume to have been created already (default).
  `write-disposition`  controls whether data should :append (default),
  :truncate or :empty to fail if table exists.
  :format              :json or :csv
  :schema              sequence describing the table schema.[{:name \"foo\" :type :record :fields [{:name \"bar\" :type :integer}]}]"
  [^BigQuery service
   table
   {:keys [format create-disposition write-disposition max-bad-records schema]}
   uris]
  (let [builder (LoadJobConfiguration/newBuilder
                  ^TableId (table-id table)
                  ^List uris
                  ^FormatOptions ({:json (FormatOptions/json)
                                   :csv  (FormatOptions/csv)} (or format :json)))]
    (.setCreateDisposition builder (create-dispositions (or create-disposition :never)))
    (.setWriteDisposition builder (write-dispositions (or write-disposition :append)))
    (.setMaxBadRecords builder (int (or max-bad-records 0)))
    (when schema
      (.setSchema builder (mkschema schema)))
    (execute-job service (.build builder))))

(def extract-format {:json "NEWLINE_DELIMITED_JSON"
                     :csv  "CSV"
                     :avro "AVRO"})

(def extract-compression {:gzip "GZIP"
                          :none "NONE"})

(defn extract-job
  "Extracts data from BigQuery into a Google Cloud Storage location.
   Table argument needs to be a map with project-id, dataset-id and table-id."
  [^BigQuery service
   table
   destination-uri & {:keys [format compression]
                      :or   {format      :json
                             compression :gzip}}]
  (let [builder (ExtractJobConfiguration/newBuilder ^TableId (table-id table) ^String destination-uri)]
    (.setFormat builder (extract-format format))
    (.setCompression builder (extract-compression compression))
    (execute-job service (.build builder))))

(defn copy-job
  [^BigQuery service
   sources
   destination & {:keys [create-disposition write-disposition]
                  :or   {create-disposition :needed
                         write-disposition  :empty}}]
  (let [builder (CopyJobConfiguration/newBuilder ^TableId (table-id destination) ^List (map table-id sources))]
    (.setCreateDisposition builder (create-dispositions create-disposition))
    (.setWriteDisposition builder (write-dispositions write-disposition))
    (execute-job service (.build builder))))

(defn user-defined-function
  "Creates a User Defined Function suitable for use in BigQuery queries. Can be a Google Cloud Storage uri (e.g. gs://bucket/path), or an inline JavaScript code blob."
  [udf]
  (if (.startsWith udf "gs://")
    (UserDefinedFunction/fromUri udf)
    (UserDefinedFunction/inline udf)))

(defn query-job
  [^BigQuery service
   query
   {:keys [create-disposition write-disposition large-results?
           dry-run? destination-table default-dataset use-cache?
           flatten-results? use-legacy-sql? priority udfs]}]
  (let [priorities {:batch       (QueryJobConfiguration$Priority/BATCH)
                    :interactive (QueryJobConfiguration$Priority/INTERACTIVE)}
        builder (QueryJobConfiguration/newBuilder query)]
    (when default-dataset
      (let [{:keys [project-id dataset-id]} default-dataset]
        (.setDefaultDataset builder (DatasetId/of project-id dataset-id))))
    (.setCreateDisposition builder (create-dispositions (or create-disposition :never)))
    (.setWriteDisposition builder (write-dispositions (or write-disposition :append)))
    (.setUseLegacySql builder use-legacy-sql?)
    (.setAllowLargeResults builder large-results?)
    (.setUseQueryCache builder use-cache?)
    (.setFlattenResults builder flatten-results?)
    (.setPriority builder (priorities (or priority :batch)))
    (when udfs
      (.setUserDefinedFunctions builder udfs))
    (when destination-table
      (let [{:keys [project-id dataset-id table-id]} destination-table]
        (.setDestinationTable builder (TableId/of project-id dataset-id table-id))))
    (when-not (nil? dry-run?)
      (.setDryRun builder dry-run?))
    (execute-job service (.build builder))))

(comment
  (def bigquery-service
    (service
      {:project-id "amp-compute"}))

  (def transform-dataset
    {:max-wait-millis (* 60 1000)
     :default-dataset {:project-id "uswitch-ldn"
                       :dataset-id "dbt_transform"}})

  (def dbt-gold
    {:max-wait-millis (* 60 1000)
     :default-dataset {:project-id "uswitch-ldn"
                       :dataset-id "dbt_gold"}})

  (query bigquery-service
         (str "#standardSQL\n"
              "SELECT DISTINCT company_id, company_name "
              "FROM companies_latest "
              "ORDER BY company_name LIMIT 10")
         dbt-gold)
  )
