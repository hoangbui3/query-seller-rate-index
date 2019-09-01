package vn.tiki.discovery.crawler;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import org.threeten.bp.Duration;
import vn.tiki.discovery.utils.CommonUtils;

import java.io.*;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

public class BigQueryCrawler {
    private final BigQuery bqConnection;
	private final String bqQueryTemplate;

    public BigQueryCrawler() throws IOException {
        InputStream fileInputStream = this.getClass().getClassLoader().getResourceAsStream("tiki-search-platform.json");
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

