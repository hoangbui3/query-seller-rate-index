package vn.tiki.discovery;

import vn.tiki.discovery.crawler.BigQueryCrawler;
import vn.tiki.discovery.crawler.GoogleStorageCrawler;
import vn.tiki.discovery.utils.CommonUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class BulkUpload {
    public static final String JOB_UPLOAD_GS = "upload-gs";
    public static final String JOB_INSERT_ES = "insert-es";

    private static BigQueryCrawler bigQueryCrawler;
    private static GoogleStorageCrawler googleStorageCrawler;
	private static ElasticSearchClient elasticSearchClient;

	private static final long ONE_DAY_IN_MILLIS = 24 * 60 * 60 * 1000;
	private static String yesterdayDateSuffix() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		return sdf.format(new Date(System.currentTimeMillis() - ONE_DAY_IN_MILLIS));
	}

    public static void main(String[] args) throws Exception {
		bigQueryCrawler = new BigQueryCrawler();
		googleStorageCrawler = new GoogleStorageCrawler();
		elasticSearchClient = new ElasticSearchClient();

        String job = args.length > 0 ? args[0] : JOB_INSERT_ES;
        String dateSuffix = args.length > 1 ? args[1] : yesterdayDateSuffix();

        if (job.equals(JOB_UPLOAD_GS)) {
			googleStorageCrawler.uploadDataToGS(
				bigQueryCrawler.getBigQueryDataResult(dateSuffix),
				"query_seller_rate/" + dateSuffix + ".tsv"
			);
        }

        if (job.equals(JOB_INSERT_ES)) {
			elasticSearchClient.insertDataToES(
				googleStorageCrawler.getDataFromGS("query_seller_rate/" + dateSuffix + ".tsv"),
				dateSuffix
			);
        }
    }
}
