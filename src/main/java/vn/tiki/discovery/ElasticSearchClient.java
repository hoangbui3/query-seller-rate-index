package vn.tiki.discovery;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
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
import vn.tiki.discovery.utils.SellerQueryPair;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

// TODO turn it into a proper singleton
// TODO change alias AFTER index is created, not BEFORE

public class ElasticSearchClient {
    public static final String INDEX_NAME = System.getenv("QUERY_SELLER_RATE_ALIAS") == null ?
            "query_seller_rate" : System.getenv("QUERY_SELLER_RATE_ALIAS");

    public static final String HOST_STR = System.getenv("ELASTIC_SEARCH_HOST") == null ?
            "uat-browser-esfive-1.svr.tiki.services" : System.getenv("ELASTIC_SEARCH_HOST");

    public static final int PORT = Integer.parseInt(System.getenv("ELASTIC_SEARCH_PORT") == null ?
            "9200" : System.getenv("ELASTIC_SEARCH_PORT"));

    public static final int MAX_SELLER_RATE_DAYS = Integer.parseInt(System.getenv("MAX_SELLER_RATE_DAYS") == null ?
            "90" : System.getenv("MAX_SELLER_RATE_DAYS"));

    private static RestHighLevelClient restHighLevelClient;


    public void insertDataToES(List<String[]> rowsList, String dateSuffix) throws IOException {
        if (rowsList == null) {
            return;
        }
        makeConnection(HOST_STR.split("\t"), PORT);

        try {
            // starttime of function
            long startTime = System.currentTimeMillis();
            List<String> list = getOldAlias(INDEX_NAME);
            String indexName = INDEX_NAME + "_" + dateSuffix + "_" + (new Random().nextInt(1000));
            int retry = 0;
            boolean isCreated = false;
            while (!isCreated && retry < 3) {
                isCreated = createIndex(indexName);
                if (isCreated) {
                    System.out.println("Create Index " + indexName + " Success!");
                    boolean isInsertSuccess = ESBulkInsert(indexName, rowsList);
                    if (isInsertSuccess) {
                        createAlias(indexName);
                    } else {
                        list.clear();
                        list.add(indexName);
                    }
                    removeOldIndex(list);
                }

                retry++;
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


    private HashMap<String, Integer>[] createWordsFrequencyHashmap(LinkedHashMap<String, SellerQueryPair> queryHashMap,
                                                                   HashMap<String, Integer>[] frequencyHashMap, boolean isCheckRepeat) {

        if (frequencyHashMap == null) {
            frequencyHashMap = new LinkedHashMap[3];
            for (int i = 0; i < frequencyHashMap.length; i++) {
                frequencyHashMap[i] = new LinkedHashMap<>();
            }
        }

        HashMap<String, Integer>[] repeatHashmap = new LinkedHashMap[3];
        for (int i = 0; i < repeatHashmap.length; i++) {
            repeatHashmap[i] = new LinkedHashMap<>();
        }
        for (String query : queryHashMap.keySet()) {
            SellerQueryPair pair = queryHashMap.getOrDefault(query, new SellerQueryPair());

            String[] words = query.split("\\s+");
            for (int i = 0; i < words.length; i++) {
                int frequency = frequencyHashMap[0].getOrDefault(words[i], 0);
                frequencyHashMap[0].put(words[i], frequency + pair.sellerCountClick);

                repeatHashmap[0].put(words[i], repeatHashmap[0].getOrDefault(words[i], 0) + 1);


                if (words.length > 1 && i < words.length - 1) {
                    String word2 = words[i] + " " + words[i + 1];
                    frequency = frequencyHashMap[1].getOrDefault(word2, 0);
                    frequencyHashMap[1].put(word2, frequency + pair.sellerCountClick);

                    repeatHashmap[1].put(word2, repeatHashmap[1].getOrDefault(word2, 0) + 1);

                }

                if (words.length > 2 && i < words.length - 2) {
                    String word3 = words[i] + " " + words[i + 1] + " " + words[i + 2];
                    frequency = frequencyHashMap[2].getOrDefault(word3, 0);
                    frequencyHashMap[2].put(word3, frequency + pair.sellerCountClick);

                    repeatHashmap[2].put(word3, repeatHashmap[2].getOrDefault(word3, 0) + 1);

                }

            }

        }

        if (isCheckRepeat) {
            Set<String>[] removeHashmap = new Set[3];
            for (int i = 0; i < removeHashmap.length; i++) {
                removeHashmap[i] = new HashSet<>();
            }

            for (int i = 0; i < repeatHashmap.length; i++) {
                for (String word : repeatHashmap[i].keySet()) {
                    int repeat = repeatHashmap[i].getOrDefault(word, 0);

                    if (1f * repeat / queryHashMap.size() <= 0.5) {
                        removeHashmap[i].add(word);
                    }
                }
            }
            for (int i = 0; i < repeatHashmap.length; i++) {
                for (String removeWord : removeHashmap[i]) {
                    frequencyHashMap[i].remove(removeWord);
                }
            }
        }

        HashMap<String, Integer>[] sortedFrequencyHashMap = new LinkedHashMap[3];
        for (int i = 0; i < frequencyHashMap.length; i++) {
            sortedFrequencyHashMap[i] = frequencyHashMap[i]
                    .entrySet()
                    .stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, LinkedHashMap::new));
        }

        return sortedFrequencyHashMap;

    }

    private synchronized RestHighLevelClient makeConnection(String[] hosts, int port) {
        if (restHighLevelClient != null) {
            return restHighLevelClient;
        }

        HttpHost[] httpHosts = new HttpHost[hosts.length];
        int count = 0;
        for (String host : hosts) {
            System.out.println("HOST: " + hosts[count].trim() + ":" + port);
            httpHosts[count] = new HttpHost(hosts[count].trim(), port);
            count++;
        }

        restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts));
        return restHighLevelClient;
    }

    private synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    private synchronized List<String> getOldAlias(String name) throws IOException {
        GetAliasesRequest request = new GetAliasesRequest(name);
        GetAliasesResponse response = restHighLevelClient.indices().getAlias(request, RequestOptions.DEFAULT);

        Map<String, Set<AliasMetaData>> map = response.getAliases();
        List<String> aliasIndex = new ArrayList<>();
        for (String index : map.keySet()) {
            aliasIndex.add(index);
        }
        return aliasIndex;
    }

    private synchronized void removeOldIndex(List<String> listRemove) throws IOException {
        if (listRemove != null && !listRemove.isEmpty()) {
            for (String indexName : listRemove) {
                System.out.println("Detele index:" + indexName);

                DeleteIndexRequest request = new DeleteIndexRequest(indexName);
                AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            }
        }
    }

    private synchronized boolean createIndex(String indexName) throws IOException {
        String indexSettingsAndMappings = CommonUtils.readAll(ElasticSearchClient.class.getClassLoader().getResourceAsStream("query_seller_rate_settings_and_mappings.json"));
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.source(indexSettingsAndMappings, XContentType.JSON);
/*
        request.alias(new Alias(INDEX_NAME));
*/

        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        return createIndexResponse.isAcknowledged();
    }

    private synchronized boolean createAlias(String indexName) throws IOException {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .index(indexName)
                        .alias(INDEX_NAME);
        request.addAliasAction(aliasAction);

        AcknowledgedResponse indicesAliasesResponse =
                restHighLevelClient.indices().updateAliases(request, RequestOptions.DEFAULT);
        System.out.println("create alias:" + INDEX_NAME + " with index: " + indexName);

        return indicesAliasesResponse.isAcknowledged();
    }

    public static final float[] FREQUENCY_RATE_THRESHOLD = {0.6f, 0.4f, 0.2f};
    public static final int[] FREQUENCY_THRESHOLD = {100, 100, 50};

    private String[] getSellerKeyName(LinkedHashMap<String, SellerQueryPair> queryHashMap, HashMap<String, Integer>[] frequencyHashMap) {

        HashMap<String, Integer>[] innerFrequencyHashmap = createWordsFrequencyHashmap(queryHashMap, null, true);
        List<String> keyList = new ArrayList<>();
        float maxKeywordRate = 0f;
        for (int i = 0; i < 2; i++) {
            int count = 0;
            String keyword = null;
            for (String candidate : innerFrequencyHashmap[i].keySet()) {

                int candidateFrequency = frequencyHashMap[i].getOrDefault(candidate, 0);
                int innerCandidateFrequency = innerFrequencyHashmap[i].getOrDefault(candidate, 0);
                if (candidateFrequency > 0) {
                    float frequencyRate = 1f * innerCandidateFrequency / candidateFrequency;
                    if (frequencyRate > FREQUENCY_RATE_THRESHOLD[i]
                            && innerCandidateFrequency > FREQUENCY_THRESHOLD[i]
                            && frequencyRate * (i + 1) > maxKeywordRate) {
                        maxKeywordRate = frequencyRate * (i + 1);
                        keyword = candidate;
                    }
                }

                count++;
                if (count > 3 - i) {
                    break;
                }
            }

            if (keyword != null) {
                keyList.add(keyword);
            }
        }

        return keyList.toArray(new String[keyList.size()]);
    }

    public synchronized boolean ESBulkInsert(String indexName, List<String[]> rowsList) throws Exception {
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

        //pre filter, support merging N days data from GS into one.
        // new rate = (total count store click of 90 days/ total of 90 days)

        LinkedHashMap<String, LinkedHashMap<String, SellerQueryPair>> sellerHashMap = new LinkedHashMap<>();

        for (int i = 0; i < rowsList.size(); ++i) {
            String[] fields = rowsList.get(i);
            int currentID = Integer.valueOf(fields[2]);
            String query = fields[0];
            String sellerID = fields[2];

            LinkedHashMap<String, SellerQueryPair> queryHashMap = sellerHashMap.getOrDefault(sellerID, new LinkedHashMap<>());
            SellerQueryPair pair = queryHashMap.getOrDefault(query, new SellerQueryPair());
            pair.total += Integer.valueOf(fields[3]);
            pair.sellerCountClick += Integer.valueOf(fields[4]);

            pair.sellerName = fields[1];
            pair.sellerID = sellerID;

            queryHashMap.put(query, pair);
            sellerHashMap.put(sellerID, queryHashMap);
        }
        HashMap<String, Integer>[] frequencyHashmap = null;
        for (String sellerID : sellerHashMap.keySet()) {
            LinkedHashMap<String, SellerQueryPair> queryHashMap = sellerHashMap.getOrDefault(sellerID, new LinkedHashMap<>());
            frequencyHashmap = createWordsFrequencyHashmap(queryHashMap, frequencyHashmap, false);
        }

        for (String key : frequencyHashmap[0].keySet()) {
            if (frequencyHashmap[0].get(key) < 50) {
                break;
            }
            System.out.println(key + " has frequency: " + frequencyHashmap[0].get(key));
        }


        int sellerSize = sellerHashMap.size();
        int i = 0;

        for (String sellerID : sellerHashMap.keySet()) {
            boolean isLast = false;
            int j = 0;
            LinkedHashMap<String, SellerQueryPair> queryHashMap = sellerHashMap.getOrDefault(sellerID, new LinkedHashMap<>());

            String[] keyList = getSellerKeyName(queryHashMap, frequencyHashmap);
            for (String query : queryHashMap.keySet()) {

                if (i == sellerSize - 1) {
                    if (j == queryHashMap.size() - 1) {
                        isLast = true;
                    }
                }

                SellerQueryPair pair = queryHashMap.getOrDefault(query, new SellerQueryPair());

                try {
                    int currentID = Integer.valueOf(sellerID);
                    String sellerName = pair.sellerName;
                    String rate = Float.toString((float) pair.sellerCountClick / pair.total);

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
                        jb.field("seller_name", sellerName);
                        jb.field("seller_id", sellerID);
                        if (keyList != null && keyList.length > 0) {
                            jb.field("key_list", keyList);
                        }
                        jb.startArray("queries");

                        preID = currentID;
                    }

                    jb.startObject();
                    jb.field("query", query);
                    jb.field("rate", rate);
                    jb.endObject();

                    if (isLast) {

                        jb.endArray();
                        jb.endObject();

                        String id = UUID.randomUUID().toString();
                        bulkRequest1.add(new IndexRequest(indexName, "seller", id).source(jb));
                    }

                    // checking if counter is divisible by batch, to make us aware of how many batches have been processed
                    // and create a new BulkRequest object
                    if ((counter != preCounter && counter % batch == 0) || isLast) {
                        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest1, RequestOptions.DEFAULT);
                        bulkRequest1 = new BulkRequest();
                        System.out.println("Uploaded: " + counter + " sellers so far");
                        preCounter = counter;
                    }
                } catch (Exception ex) {
                    System.out.println("error on row: " + i);
                    ex.printStackTrace();
                    return false;
                }
                j++;
            }
            i++;
        }

        return true;
    }
}

