package clawer;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import com.opencsv.CSVWriter;
import utils.CommonUtils;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

public class GoogleStorageClawer {
    private String fileName;
    private String postfix;

    private Storage storage;
    public static String BUCKET = "tiki_search_platform";
    public static final String BASE_FILE_NAME = "query_seller_rate";

    public GoogleStorageClawer(Date date) throws IOException {

        InputStream fileInputStream = this.getClass().getClassLoader().getResourceAsStream("tiki-search-platform.json");
        GoogleCredentials credentials = GoogleCredentials.fromStream(fileInputStream);
        storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId("tiki-dwh")
                .build()
                .getService();

        String myCredentials = "/path/to/my/key.json";
        this.postfix = CommonUtils.getStringByDate(date);
        fileName = BASE_FILE_NAME + "_" + postfix + ".csv";
    }

    public BufferedReader getDataFromGS(String bucket, String fileName) throws Exception {

        BlobId blobId = BlobId.of(bucket, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        Blob blob = storage.get(blobId);
        if (blob == null) {
            System.out.println("File " + fileName + " not found on Google Storage");
            return null;
        }
        ReadChannel readChannel = blob.reader();
        System.out.println("Start reading " + fileName + " from Google Storage");
        BufferedReader br = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));

        return br;

    }


    public void uploadDataToGS(String filePath) throws IOException, InterruptedException {

        byte[] fileContent = Files.readAllBytes(Paths.get(filePath));

        BlobId blobId = BlobId.of(GoogleStorageClawer.BUCKET, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        System.out.println("Start uploading "+fileName+" to Google Storage");
        Blob blob = storage.create(blobInfo, fileContent);
        System.out.println("Upload "+fileName+" to Google Storage done");

    }



    public static final String[] COLS = new String[]{"query", "seller_name", "seller_id", "rate"};

    public void saveResultToTempFile(TableResult result) throws IOException {
        if (result == null) {
            return;
        }
        String directory = "./temp";
        System.out.println("Start writing result to CVS temp file");
        File file = new File(directory, fileName);

        // if file does not exists, then create it
        if (!file.exists()) {
            //clean temp folder
            File dir = new File(directory);

            for (File oldFile: dir.listFiles())
                if (!file.isDirectory())
                    file.delete();
            //create new file for today
            file.createNewFile();
        }else{
            return;
        }



        FileWriter csvWriter = new FileWriter(file.getAbsoluteFile());
        CSVWriter writer = new CSVWriter(csvWriter
                , ','
                , CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END);

        writer.writeNext(COLS);

        for (FieldValueList row : result.iterateAll()) {
            String[] line = new String[4];
            int count = 0;
            for (FieldValue val : row) {
                String value = val.getValue().toString()
                        .replace("\n", " ")
                        .replace(",", " ")
                        .replace(" +", " ")
                        .trim();
                line[count++] = value;

            }
            writer.writeNext(line);
        }
        writer.close();
        System.out.println("Writing to temp file done");
    }


    public String getFileName() {
        return fileName;
    }


}
