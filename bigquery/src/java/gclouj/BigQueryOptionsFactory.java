package gclouj;

import com.google.cloud.bigquery.BigQueryOptions;

public class BigQueryOptionsFactory {
  public static BigQueryOptions create(String projectId) {
    BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
      .setProjectId(projectId);
    return builder.build();
  }
}