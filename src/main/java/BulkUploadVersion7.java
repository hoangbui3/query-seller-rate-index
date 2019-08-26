//Adding imports

import clawer.GoogleStorageClawer;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.client.indices.CreateIndexRequest;
//import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import utils.CommonUtils;
import utils.PropertiesReader;


import java.util.*;
import java.io.*;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

//for ES version 7.3.0+++
public class BulkUploadVersion7 {
    //Adding variables for ElasticSearch connection i.e "http://localhost:9200"

    public static String INDEX_NAME;
    public static String HOST;
    public static int PORT;
    public static String SCHEME;
    public static String JOB1 = "upload-gs";
    public static String JOB2 = "insert-es";

    //RestHighLevelClient Object
    private static RestHighLevelClient restHighLevelClient;
    public DataTransfer dataTransfer;


    public static void main(String[] args) throws Exception {
        BulkUploadVersion7 bulkupload = new BulkUploadVersion7();
        String job;
        Date date;

        if (args == null || args.length == 0) {
            date = new Date();
            job = JOB2;
        } else {
            job = args[0];
            if (args.length > 1) {
                date = CommonUtils.getDateByString(args[1]);
            } else {
                date = new Date();
            }
        }

        //BulkUpload class object
        bulkupload.loadProperties();
        bulkupload.dataTransfer = new DataTransfer(date);
        // makeConnection function to test whether we are connected with ElasticSearch Server or Not

        if (job.equals(JOB1)) {
            bulkupload.dataTransfer.transferDataFromBQtoGS();
        }
        if (job.equals(JOB2)) {
            List<String> valueList = bulkupload.dataTransfer.getDataFromGS();
            bulkupload.insertDataToES(valueList);
        }
    }


    public void insertDataToES(List<String> valueList) throws IOException {
        if (valueList == null) {
            return;
        }
        makeConnection();

        try {
            // starttime of function
            long startTime = System.currentTimeMillis();
            List<String> list = getOldAlias(INDEX_NAME);
            String indexName = INDEX_NAME + "_" + CommonUtils.getStringByDate(new Date()) + "_" + (new Random().nextInt(1000));
            boolean isCreated = createIndex(indexName);
            if (isCreated) {
                System.out.println("Create Index" + indexName + " Success!");
                ESBulkInsert(indexName, valueList);
                removeOldIndex(list);
            }
            // endtime
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            System.out.println("Duration: " + duration + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // closeConnection function to close connection with ES after Insertion.
        closeConnection();
    }

    private synchronized RestHighLevelClient makeConnection() {
        System.out.println("Making ES Connection");

        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new org.apache.http.HttpHost(HOST, PORT, SCHEME)
                    ));
        }
        return restHighLevelClient;
    }

    private static synchronized void closeConnection() throws IOException {
        // Closing the client connection with .close()
        System.out.println("Close ES connection");
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    private static synchronized List<String> getOldAlias(String name) throws IOException {
        GetAliasesRequest request = new GetAliasesRequest(name);

        GetAliasesResponse response = restHighLevelClient.indices().getAlias(request, RequestOptions.DEFAULT);

        Map<String, Set<AliasMetaData>> map = response.getAliases();
        List<String> aliasIndex = new ArrayList<>();
        for (String index : map.keySet()) {
            aliasIndex.add(index);
        }
        return aliasIndex;


    }

    private static synchronized void removeOldIndex(List<String> listRemove) throws IOException {
        if (listRemove != null && !listRemove.isEmpty()) {
            for (String indexName : listRemove) {
                System.out.println("Detele index:" + indexName);

                DeleteIndexRequest request = new DeleteIndexRequest(indexName);
                AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            }
        }
    }

    private synchronized boolean createIndex(String indexName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        request.mapping(
                "{\n" +
                        "    \"properties\": {\n" +
                        "      \"seller_id\": {\"type\": \"long\"},\n" +
                        "      \"seller_name\": {\"type\": \"text\"},\n" +
                        "      \"queries\":{\n" +
                        "        \"type\": \"nested\",\n" +
                        "        \"properties\": {\n" +
                        "          \"query\": {\"type\": \"text\"},\n" +
                        "          \"rate\": {\"type\": \"float\"}\n" +
                        "        }\n" +
                        "      }\n" +
                        "  }\n" +
                        "}\n",
                XContentType.JSON);
        request.alias(new Alias(INDEX_NAME));

        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        return createIndexResponse.isAcknowledged();
    }


    public void loadProperties() {
        Properties prop = PropertiesReader.getProp();
        HOST = prop.getProperty("HOST");
        PORT = Integer.valueOf(prop.getProperty("PORT"));
        SCHEME = prop.getProperty("SCHEME");
        INDEX_NAME = prop.getProperty("INDEX_NAME");
    }

    public synchronized void ESBulkInsert(String indexName, List<String> valueList) throws Exception {


        // TSV File Connection
//        BufferedReader CSVFile = new BufferedReader(new FileReader("src/main/resources/16kReal.csv"));
//        List<String> valueList = dataTransfer.getDataFromGS(CSVFile);
//        CSVFile.close();

        StringTokenizer st; // StringTokenizer object to break our dataRows from tsv to tokens

        // Columns count
        int colCount = GoogleStorageClawer.COLS.length;
        System.out.println("colCount: " + colCount);
        System.out.println("size: " + valueList.size());

        // Creating a new BulkRequest object with the name bulkRequest1
        BulkRequest bulkRequest1 = new BulkRequest();

        // Setting batch value to make requests in batches since our number of rows can be in millions or more than that
        int batch = 1000;
        // Conter to check how many rows have been processed
        int counter = 0;
        int preCounter = 0;
        int preID = 0;


        XContentBuilder jb = XContentFactory.jsonBuilder();
        jb.startObject();
        for (int i = 0; i < valueList.size(); i += colCount) {
            try {

                // Increment everytime we process a row
                // Generating random IDs for each document

                int currentID = Integer.valueOf(valueList.get(i + 2));
                String query = valueList.get(i).toLowerCase();
                String rate = valueList.get(i + 3);
                String sellerName = valueList.get(i + 1);
                if (currentID != preID) {
                    if (preID != 0) {
                        jb.endArray();
                        jb.endObject();

                        String id = UUID.randomUUID().toString();
                        bulkRequest1.add(new IndexRequest(indexName).id(id)
                                .source(jb));
                        System.out.println(i + "." + counter + "." + sellerName);

                        counter++;
                    }

                    jb = XContentFactory.jsonBuilder();
                    jb.startObject();
                    jb.field("seller_id", valueList.get(i + 2));
                    jb.field("seller_name", valueList.get(i + 1));
                    jb.startArray("queries");

                    preID = currentID;
                }

                jb.startObject();
                jb.field("query", query);
                jb.field("rate", rate);
                jb.endObject();

                if (i == valueList.size() - colCount) {
                    jb.endArray();
                    jb.endObject();

                    String id = UUID.randomUUID().toString();
                    bulkRequest1.add(new IndexRequest(indexName).id(id)
                            .source(jb));
                }


                // checking if counter is divisible by batch, to make us aware of how many batches have been processed
                // and create a new BulkRequest object
                if ((counter != preCounter && counter % batch == 0) || i == valueList.size() - colCount) {
                    BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest1, RequestOptions.DEFAULT);
                    bulkRequest1 = new BulkRequest();
                    System.out.println("Uploaded: " + counter + " sellers so far");
                    preCounter = counter;
                }
            } catch (Exception ex) {
                System.out.println("error on row: " + i);
                ex.printStackTrace();
                return;
            }
        }
    }

}
