package vn.tiki.discovery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GoogleStorageClient {
    public static final String BUCKET_NAME = "tiki_search_platform";
    public static final String BASE_FILE_NAME = "query_seller_rate";
    private final Storage storage;

    public GoogleStorageClient() throws IOException {
        InputStream fileInputStream = this.getClass().getClassLoader().getResourceAsStream("tiki-search-platform.json");
        GoogleCredentials credentials = GoogleCredentials.fromStream(fileInputStream);
        this.storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId("tiki-dwh")
                .build()
                .getService();
    }

	public List<String[]> getDataFromGS(String fileName) throws IOException, InterruptedException {
		BlobId blobId = BlobId.of(BUCKET_NAME, fileName);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
		Blob blob = storage.get(blobId);
		if (blob == null) {
			System.out.println("File " + fileName + " not found on Google Storage");
			return null;
		}
		ReadChannel readChannel = blob.reader();
		System.out.println("Start reading " + fileName + " from Google Storage");
		BufferedReader br = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));

		List<String[]> result = new ArrayList<>();

		String line;
		while ((line = br.readLine()) != null) {
			String[] fields = line.split("\t");
			result.add(fields);
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
