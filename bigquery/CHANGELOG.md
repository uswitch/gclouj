1.0.0

# Breaking
- remove `query-results` and `query-results-seq`. `query` now directly return the results of the query, i.e., it acts
  as `(comp query-results-seq query)`. This is largely to do with the underlying BQ library having moved on to
  synchronous calls in the 5 years we haven't bothered updating our proxy.
- possibly absolutely **everything** else bar `query`, there are no tests in this repo, and whilst I've done my best,
  Clojure is not even a compiled language, so I've no idea what I've trampled. Re-test your apps if you boop to 1.0+
  and use this lib to insert data or create tables or do anything else fun like that.

# Non breaking
- adds initial support for BigQuery Storage API querying
