package vn.tiki.discovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import vn.tiki.discovery.crawler.BigQueryCrawler;
import vn.tiki.discovery.crawler.GoogleStorageCrawler;

public class DataTransfer {
    private BigQueryCrawler bigQueryCrawler;
    private GoogleStorageCrawler googleStorageCrawler;

    public DataTransfer(Date date) throws IOException {
        bigQueryCrawler = new BigQueryCrawler(date);
        googleStorageCrawler = new GoogleStorageCrawler(date);
    }

    public void transferQueryToGS() throws IOException, InterruptedException {
    }

    public void transferDataFromBQtoGS() throws IOException, InterruptedException {
        String directory = "./temp";
        File file = new File(directory, googleStorageCrawler.getFileName());

        if (!file.exists()) {
            System.out.println("File  not exits, query GoogleBigQuery then Save to temp file");
            googleStorageCrawler.saveResultToTempFile(bigQueryCrawler.getBigQueryDataResult());
        } else {
            System.out.println("File exits, upload it to Google Storage");
        }
        googleStorageCrawler.uploadDataToGS(file.getAbsolutePath());
    }

    public List<String> getDataFromGS() throws IOException {
        BufferedReader CSVFile = null;
        try {
            int retry = 0;
            while (CSVFile == null) {
                CSVFile = googleStorageCrawler.getDataFromGS(
                        googleStorageCrawler.BUCKET, googleStorageCrawler.getFileName());
                if (CSVFile == null) {
                    transferDataFromBQtoGS();
                    retry++;
                }
                if (retry == 3) {
                    return null;
                }
            }

            List<String> getCVSValue = getCVSValue(CSVFile);
            return getCVSValue;
        } catch (Exception ex) {
            ex.printStackTrace();

        } finally {
            if (CSVFile != null) {
                CSVFile.close();
            }
        }
        return null;

    }


    public List<String> getCVSValue(BufferedReader CSVFile) throws Exception {
        System.out.println("Start parsing CSV file");

        StringTokenizer st; // StringTokenizer object to break our dataRows from tsv to tokens

        String dataRow = CSVFile.readLine();

        // each token separated by a tab, hence "\t"
        st = new StringTokenizer(dataRow, ",");

        // ArrayList to store keys from tsv i.e our column names.
        List<String> keyArray = new ArrayList<>();

        // Loop to check if StringTokenizer has more tokens.
        while (st.hasMoreElements()) {

            // Converting the next token to String
            String element = st.nextElement().toString();
            // Adding element to keyArray
            keyArray.add(element);
        } // Column names  added in the keyList

        // valueList array to store values inside the columns
        List<String> valueList = new ArrayList<>();

        // Loop till the last dataRow i.e list of values in each column
        while ((dataRow = CSVFile.readLine()) != null) {
            // breaking down data row as tokens, taking tabs("\t") as separator
            st = new StringTokenizer(dataRow, ",");

            // adding values to the valueList by converting tokens to String
            while (st.hasMoreElements()) {
                valueList.add(st.nextElement().toString());
            }
        }
        System.out.println("Parsing CSV file done");

        // Closing tsc file connection
        return valueList;
    }

}