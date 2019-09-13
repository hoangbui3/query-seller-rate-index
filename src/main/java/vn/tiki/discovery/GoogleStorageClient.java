package vn.tiki.discovery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class GoogleStorageClient {
    public static final String BUCKET_NAME = "tiki_search_platform";
    public static final String BASE_FILE_NAME = "query_seller_rate";
    private final Storage storage;
    private String dateSuffix;

    public GoogleStorageClient() throws IOException {
        InputStream fileInputStream = new FileInputStream("./gcp/bigquery/tiki-search-platform.json");

        GoogleCredentials credentials = GoogleCredentials.fromStream(fileInputStream);
        this.storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId("tiki-dwh")
                .build()
                .getService();
    }

    public List<String[]> getDataFromGS(Date date) throws IOException, InterruptedException {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        List<String[]> result = new ArrayList<>();

        for (int i = 0; i < ElasticSearchClient.MAX_SELLER_RATE_DAYS; i++) {
            calendar.add(Calendar.DATE, (i == 0) ? 0 : -1);
            String dateSuffix = BulkUpload.getDateSuffix(calendar.getTime());

            String fileName = "query_seller_rates/" + dateSuffix + ".tsv";
            BlobId blobId = BlobId.of(BUCKET_NAME, fileName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
            Blob blob = storage.get(blobId);
            if (blob == null) {
                System.out.println("File " + fileName + " not found on Google Storage");
                continue;
            }
            ReadChannel readChannel = blob.reader();
            System.out.println("Start reading " + fileName + " from Google Storage");
            BufferedReader br = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));

            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split("\t");
                result.add(fields);
            }
        }

        return result;
    }

    public void uploadDataToGS(TableResult data, String fileName) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (FieldValueList row : data.iterateAll()) {
            boolean isFirst = true;
            for (FieldValue v : row) {
                if (!isFirst) {
                    sb.append("\t");
                }
                sb.append(v.getStringValue());
                isFirst = false;
            }
            sb.append("\n");
        }

        byte[] sbBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        BlobId blobId = BlobId.of(BUCKET_NAME, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        System.out.println("Start uploading " + fileName + " to Google Storage");
        Blob blob = storage.create(blobInfo, sbBytes);
        System.out.println("Upload " + fileName + " to Google Storage done");
    }
}
