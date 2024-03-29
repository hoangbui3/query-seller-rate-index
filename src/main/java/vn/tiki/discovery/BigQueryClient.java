package vn.tiki.discovery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import vn.tiki.discovery.utils.CommonUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public class BigQueryClient {
    private final BigQuery bqConnection;
	private final String bqQueryTemplate;

    public BigQueryClient() throws IOException {
        InputStream fileInputStream = new FileInputStream("./gcp/bigquery/tiki-search-platform.json");

        GoogleCredentials credentials = GoogleCredentials.fromStream(fileInputStream);
        this.bqConnection = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId("tiki-dwh")
                .build().getService();
		this.bqQueryTemplate = CommonUtils.readAll(this.getClass().getClassLoader().getResourceAsStream("query_to_seller_rate.sql"));
    }

    public TableResult getBigQueryDataResult(String dateSuffix) throws InterruptedException, IOException {
		String bqQueryText = this.bqQueryTemplate.replace("{{ ds_nodash }}", dateSuffix);
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(bqQueryText)
				.setUseLegacySql(false).build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        System.out.println("Start Querying Google Big Query\n...");
        Job queryJob = bqConnection.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        return queryJob.getQueryResults();
    }
}

