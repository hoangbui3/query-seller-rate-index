package vn.tiki.discovery;

import org.apache.http.HttpHost;
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
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import vn.tiki.discovery.utils.CommonUtils;

import java.io.IOException;
import java.util.*;

// TODO turn it into a proper singleton
// TODO change alias AFTER index is created, not BEFORE

public class ElasticSearchClient {
    public static final String INDEX_NAME = System.getenv("QUERY_SELLER_RATE_ALIAS");
    public static final String HOST_STR = System.getenv("ELASTIC_SEARCH_HOST");
    public static final int PORT = Integer.parseInt(System.getenv("ELASTIC_SEARCH_PORT"));

    private static RestHighLevelClient restHighLevelClient;

    public static void insertDataToES(List<String[]> rowsList, String dateSuffix) throws IOException {
        if (rowsList == null) {
            return;
        }

        makeConnection(HOST_STR.split("\t"), PORT);

        try {
            // starttime of function
            long startTime = System.currentTimeMillis();
            List<String> list = getOldAlias(INDEX_NAME);
            String indexName = INDEX_NAME + "_" + dateSuffix + "_" + (new Random().nextInt(1000));
            boolean isCreated = createIndex(indexName);
            if (isCreated) {
                System.out.println("Create Index " + indexName + " Success!");
                ESBulkInsert(indexName, rowsList);
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

    private static synchronized RestHighLevelClient makeConnection(String[] hosts, int port) {
		if (restHighLevelClient != null) {
			return restHighLevelClient;
		}

		HttpHost[] httpHosts = new HttpHost[hosts.length];
		int count = 0;
		for (String host : hosts) {
			httpHosts[count] = new HttpHost(hosts[count].trim(), port);
			count++;
		}

        restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts));
		return restHighLevelClient;
    }

    private static synchronized void closeConnection() throws IOException {
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

    private static synchronized boolean createIndex(String indexName) throws IOException {
		String indexSettingsAndMappings = CommonUtils.readAll(ElasticSearchClient.class.getClassLoader().getResourceAsStream("query_seller_rate_settings_and_mappings.json"));
        CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.source(indexSettingsAndMappings, XContentType.JSON);
        request.alias(new Alias(INDEX_NAME));

        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        return createIndexResponse.isAcknowledged();
    }

    public static synchronized void ESBulkInsert(String indexName, List<String[]> rowsList) throws Exception {
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

		for (int i = 0; i < rowsList.size(); ++i) {
			try {
				String[] fields = rowsList.get(i);
				int currentID = Integer.valueOf(fields[2]);
				String query = fields[0];
				String sellerName = fields[1];
				String rate = fields[3];

                if (currentID != preID) {
                    if (preID != 0) {
                        jb.endArray();
                        jb.endObject();

                        String id = UUID.randomUUID().toString();
                        bulkRequest1.add(new IndexRequest(indexName, "seller", id).source(jb));
                        System.out.println(i + "." + counter + "." + sellerName);
                        counter++;
                    }

                    jb = XContentFactory.jsonBuilder();
                    jb.startObject();
                    jb.field("seller_name", fields[1]);
                    jb.field("seller_id", fields[2]);
                    jb.startArray("queries");

                    preID = currentID;
                }

                jb.startObject();
                jb.field("query", query);
                jb.field("rate", rate);
                jb.endObject();

                if (i == rowsList.size() - 1) {
                    jb.endArray();
                    jb.endObject();

                    String id = UUID.randomUUID().toString();
                    bulkRequest1.add(new IndexRequest(indexName, "seller", id).source(jb));
                }

                // checking if counter is divisible by batch, to make us aware of how many batches have been processed
                // and create a new BulkRequest object
                if ((counter != preCounter && counter % batch == 0) || i == rowsList.size() - 1) {
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
