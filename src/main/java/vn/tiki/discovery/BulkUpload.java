package vn.tiki.discovery;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BulkUpload {
    public static final String JOB_UPLOAD_GS = "upload-gs";
    public static final String JOB_INSERT_ES = "insert-es";

    private static BigQueryClient bigQueryClient;
    private static GoogleStorageClient googleStorageClient;
    private static ElasticSearchClient elasticSearchClient;

    private static final long ONE_DAY_IN_MILLIS = 24 * 60 * 60 * 1000;

    public static String getDateSuffix(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(date);
    }

    public static Date parseDate(String format) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            return sdf.parse(format);
        } catch (ParseException e) {
            e.printStackTrace();
            return new Date();
        }
    }


    public static void main(String[] args) throws Exception {
        bigQueryClient = new BigQueryClient();
        googleStorageClient = new GoogleStorageClient();
        elasticSearchClient = new ElasticSearchClient();

        String job = args.length > 0 ? args[0] : JOB_INSERT_ES;
        String dateSuffix = args.length > 1 ? args[1] : getDateSuffix(new Date(System.currentTimeMillis() - ONE_DAY_IN_MILLIS));

        if (job.equals(JOB_UPLOAD_GS)) {

			googleStorageClient.uploadDataToGS(
					bigQueryClient.getBigQueryDataResult(dateSuffix),
					"query_seller_rates/" + dateSuffix + ".tsv"
			);

        }

        if (job.equals(JOB_INSERT_ES)) {
            elasticSearchClient.insertDataToES(
                    googleStorageClient.getDataFromGS(parseDate(dateSuffix)),
                    dateSuffix
            );
        }
    }
}
