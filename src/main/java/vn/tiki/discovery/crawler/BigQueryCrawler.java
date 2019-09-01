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
    private BigQuery bigquery;

    public String bigQuery = "WITH\n" +
            "  click_product AS (\n" +
            "  SELECT\n" +
            "    keyword,\n" +
            "    product_id\n" +
            "  FROM\n" +
            "    `tiki-dwh.search_metrics_overall.base_*`\n" +
            "  WHERE\n" +
            "    _TABLE_SUFFIX BETWEEN \"fromQuery\"\n" +
            "    AND \"toQuery\"\n" +
            "    AND action = 'click on result'\n" +
            "    AND product_id IS NOT NULL),\n" +
            "  click_product_seller AS (\n" +
            "  SELECT\n" +
            "    click_product.product_id,\n" +
            "    seller_id,\n" +
            "    seller_name,\n" +
            "    keyword\n" +
            "  FROM\n" +
            "    `tiki-dwh.dwh.dim_product_full`,\n" +
            "    click_product\n" +
            "  WHERE\n" +
            "    entity_type LIKE 'seller_%'\n" +
            "    AND click_product.product_id = IFNULL(psuper_id,\n" +
            "      IFNULL(pmaster_id,\n" +
            "        product_key)) ),\n" +
            "  count_total AS (\n" +
            "  SELECT\n" +
            "    keyword,\n" +
            "    count (*) AS total\n" +
            "  FROM\n" +
            "    click_product_seller\n" +
            "  GROUP BY\n" +
            "    keyword ),\n" +
            "  count_store_per_keyword AS (\n" +
            "  SELECT\n" +
            "    keyword,\n" +
            "    seller_name,\n" +
            "    seller_id,\n" +
            "    COUNT(*) AS count_store\n" +
            "  FROM\n" +
            "    click_product_seller\n" +
            "  GROUP BY\n" +
            "    keyword,\n" +
            "    seller_name,\n" +
            "    seller_id),\n" +
            "  final AS (\n" +
            "  SELECT\n" +
            "    count_total.keyword,\n" +
            "    seller_name,\n" +
            "    seller_id,\n" +
            "    total,\n" +
            "    count_store,\n" +
            "    (count_store/total) AS rate\n" +
            "  FROM\n" +
            "    count_total,\n" +
            "    count_store_per_keyword\n" +
            "  WHERE\n" +
            "    count_total.keyword = count_store_per_keyword.keyword\n" +
            "    AND total>100\n" +
            "    AND seller_name != 'Tiki Trading'\n" +
            "  ORDER BY\n" +
            "    rate DESC,\n" +
            "    total DESC\n" +
            "  LIMIT\n" +
            "    16000)\n" +
            "SELECT\n" +
            "  replace(keyword,\",\",\" \") as query,\n" +
            "  replace(seller_name,\",\",\" \") as seller_name,\n" +
            "  seller_id,\n" +
            "  rate\n" +
            "FROM\n" +
            "  final\n" +
            "ORDER BY\n" +
            "  seller_id";

    public BigQueryCrawler(Date date) throws IOException {

        InputStream fileInputStream = this.getClass().getClassLoader().getResourceAsStream("tiki-search-platform.json");
        GoogleCredentials credentials = GoogleCredentials.fromStream(fileInputStream);
        bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId("tiki-dwh")
                .build().getService();


        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, -1);

        String toQuery = CommonUtils.getStringByDate(calendar.getTime());

        calendar.add(Calendar.DATE, -90);
        String fromQuery = CommonUtils.getStringByDate(calendar.getTime());

        bigQuery = bigQuery
                .replace("fromQuery", fromQuery)
                .replace("toQuery", toQuery);

    }


    public TableResult getBigQueryDataResult() throws InterruptedException, IOException {

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(bigQuery)
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();


        JobId jobId = JobId.of(UUID.randomUUID().toString());
        System.out.println("Start Querying Google Big Query\n...");
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

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
        TableResult result = queryJob.getQueryResults();
        System.out.println("Querying Google Big Query done");

        return result;

    }

}

