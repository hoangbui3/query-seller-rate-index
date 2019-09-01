package vn.tiki.discovery;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import vn.tiki.discovery.utils.CommonUtils;

public class BulkUpload {
    public static final String JOB_UPLOAD_GS = "upload-gs";
    public static final String JOB_INSERT_ES = "insert-es";

    private static BigQueryClient bigQueryClient;
    private static GoogleStorageClient googleStorageClient;
	private static ElasticSearchClient elasticSearchClient;

	private static final long ONE_DAY_IN_MILLIS = 24 * 60 * 60 * 1000;
	private static String yesterdayDateSuffix() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		return sdf.format(new Date(System.currentTimeMillis() - ONE_DAY_IN_MILLIS));
	}

    public static void main(String[] args) throws Exception {
		bigQueryClient = new BigQueryClient();
		googleStorageClient = new GoogleStorageClient();
		elasticSearchClient = new ElasticSearchClient();

        String job = args.length > 0 ? args[0] : JOB_INSERT_ES;
        String dateSuffix = args.length > 1 ? args[1] : yesterdayDateSuffix();

        if (job.equals(JOB_UPLOAD_GS)) {
			googleStorageClient.uploadDataToGS(
				bigQueryClient.getBigQueryDataResult(dateSuffix),
				"query_seller_rate/" + dateSuffix + ".tsv"
			);
        }

        if (job.equals(JOB_INSERT_ES)) {
			elasticSearchClient.insertDataToES(
				googleStorageClient.getDataFromGS("query_seller_rate/" + dateSuffix + ".tsv"),
				dateSuffix
			);
        }
    }
}
