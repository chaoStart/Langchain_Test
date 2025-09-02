package com.example.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;

public class IndicatorStorage {
    private static final String ES_HOST = "http://localhost:9200";
    private static final String INDEX_NAME = "datang_docs_index1";
    private static final int EMBEDDING_DIM = 1024;
    private static final String API_URL = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/storeconfig/exec";
    private static final String OPENAI_API_URL = "http://10.3.24.46:9997/v1/embeddings";
    private static final String OPENAI_API_KEY = "123";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 1. 初始化 ES 客户端
        RestClient restClient = RestClient.builder(HttpHost.create(ES_HOST)).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient esClient = new ElasticsearchClient(transport);

        String companyName = "大唐集团苏州分公司";

        // 2. 请求外部 API 获取数据
        List<String> indicatorNameList = fetchData(companyName);
        if (indicatorNameList.isEmpty()) {
            System.out.println("⚠️ 未获取到任何指标数据");
            return;
        }
        System.out.println("指标名称: " + indicatorNameList);

        // 3. 调用 OpenAI Embedding API 获取向量
        List<List<Float>> embeddings = getEmbeddings(indicatorNameList);

        // 4. 创建索引（如果不存在）
        boolean exists = esClient.indices().exists(ExistsRequest.of(e -> e.index(INDEX_NAME))).value();
        if (!exists) {
            CreateIndexRequest createRequest = new CreateIndexRequest.Builder()
                    .index(INDEX_NAME)
                    .settings(s -> s.analysis(a -> a
                            .analyzer("ik_max_word_analyzer", aa -> aa
                                    .custom(ca -> ca.tokenizer("ik_max_word"))
                            )
                    ))
                    .mappings(m -> m.properties("company_name", p -> p.text(TextProperty.of(t -> t
                                    .analyzer("ik_max_word")
                                    .searchAnalyzer("ik_smart")
                                    .fields("keyword", f -> f.keyword(k -> k)))))
                            .properties("content", p -> p.text(TextProperty.of(t -> t
                                    .analyzer("ik_max_word")
                                    .searchAnalyzer("ik_smart")
                                    .fields("keyword", f -> f.keyword(k -> k)))))
                            .properties("embedding", p -> p.denseVector(DenseVectorProperty.of(d -> d
                                    .dims(EMBEDDING_DIM)
                                    .index(true)
                                    .similarity("cosine"))))
                    )
                    .build();
            esClient.indices().create(createRequest);
            System.out.println("✅ 已创建支持向量和 IK 分词器的索引");
        } else {
            System.out.println("⚠️ 索引已存在，跳过创建");
        }

        // 5. 批量写入数据
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (int i = 0; i < indicatorNameList.size(); i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("company_name", companyName);
            doc.put("content", indicatorNameList.get(i));
            doc.put("embedding", embeddings.get(i));
            br.operations(op -> op.index(idx -> idx.index(INDEX_NAME).document(doc)));
        }
        BulkResponse bulkResponse = esClient.bulk(br.build());
        if (bulkResponse.errors()) {
            System.err.println("⚠️ 批量写入出现错误: " + bulkResponse.items());
        } else {
            System.out.println("✅ 成功写入 " + indicatorNameList.size() + " 条文档到 Elasticsearch");
        }

        restClient.close();
    }

    // ====== 请求外部数据 API ======
    private static List<String> fetchData(String companyName) throws IOException {
        OkHttpClient client = new OkHttpClient();
        MediaType JSON = MediaType.parse("application/json; charset=utf-8");

        Map<String, Object> payload = new HashMap<>();
        payload.put("code", "");
        payload.put("name", "");
        payload.put("companyName", companyName);
        payload.put("queryType", "0");
        payload.put("starttime", "2025-07-23 00:00:00");
        payload.put("endtime", "2025-07-23 23:59:59");
        payload.put("rowList", new ArrayList<>());
        payload.put("datasetIds", new ArrayList<>());
        payload.put("columnList", Collections.singletonList("指标名称"));
        payload.put("rowPathList", new ArrayList<>());
        payload.put("columnParam", new HashMap<>());

        String json = mapper.writeValueAsString(payload);
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder().url(API_URL).post(body)
                .addHeader("Content-Type", "application/json")
                .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) throw new IOException("HTTP error: " + response);

        JsonNode root = mapper.readTree(response.body().string());
        JsonNode dataNode = root.path("data");

        List<String> indicatorList = new ArrayList<>();
        if (dataNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                JsonNode valueArray = entry.getValue();
                if (valueArray.isArray() && valueArray.size() > 0) {
                    JsonNode nameNode = valueArray.get(0).path("data").path("指标名称");
                    if (nameNode.isArray()) {
                        for (JsonNode n : nameNode) {
                            indicatorList.add(n.asText());
                        }
                    }
                }
            }
        }
        return indicatorList;
    }

    // ====== 调用 OpenAI Embedding API ======
    private static List<List<Float>> getEmbeddings(List<String> texts) throws IOException {
        OkHttpClient client = new OkHttpClient();
        MediaType JSON = MediaType.parse("application/json; charset=utf-8");

        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("input", texts);
        requestMap.put("model", "bge-large-zh-v1.5");

        String json = mapper.writeValueAsString(requestMap);
        RequestBody body = RequestBody.create(JSON, json);

        Request request = new Request.Builder().url(OPENAI_API_URL)
                .post(body)
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", "Bearer " + OPENAI_API_KEY)
                .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) throw new IOException("Embedding API error: " + response);

        JsonNode root = mapper.readTree(response.body().string());
        List<List<Float>> embeddings = new ArrayList<>();
        for (JsonNode item : root.path("data")) {
            List<Float> emb = new ArrayList<>();
            for (JsonNode val : item.path("embedding")) {
                emb.add(val.floatValue());
            }
            embeddings.add(emb);
        }
        return embeddings;
    }
}
