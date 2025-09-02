package com.example.test;

import cn.hutool.json.*;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.RestClient;
import java.io.IOException;
import java.util.*;

public class DatangDataIndexer {

    private static final String API_URL = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/storeconfig/exec";
    private static final String OPENAI_EMBEDDING_URL = "http://10.3.24.46:9997/v1/embeddings";
    private static final String OPENAI_API_KEY = "123";
    private static final String ES_URL = "http://localhost:9200";
    private static final String INDEX_NAME = "datang_docs_index3";

    public static void main(String[] args) throws IOException {

        JSONObject responseData = fetchDataFromApi();
        if (responseData == null) {
            System.err.println("❌ 请求失败，程序退出");
            return;
        }

        List<Indicator> indicators = parseIndicators(responseData);
        System.out.println("📌 提取指标数量：" + indicators.size());

        ElasticsearchClient client = new ElasticsearchClient(
                new RestClientTransport(
                        RestClient.builder(HttpHost.create(ES_URL)).build(),
                        new JacksonJsonpMapper()
                )
        );

        createIndexIfNotExists(client);
        bulkInsertToES(client, indicators);
    }

    // 1. 请求外部数据接口
    static JSONObject fetchDataFromApi() {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(API_URL);
            post.setHeader("Content-Type", "application/json");

            JSONObject payload = new JSONObject();
            payload.set("code", "");
            payload.set("name", "");
            payload.set("companyName", "大唐集团南京分公司");
            payload.set("queryType", "0");
            payload.set("starttime", "2025-07-23 00:00:00");
            payload.set("endtime", "2025-07-23 23:59:59");
            payload.set("rowList", new JSONArray());
            payload.set("datasetIds", new JSONArray());
            payload.set("columnList", new JSONArray().put("指标名称"));
            payload.set("rowPathList", new JSONArray());

            JSONObject columnParam = new JSONObject();
            columnParam.set("指标结果", new JSONObject());
            columnParam.set("指标名称", new JSONObject());
            payload.set("columnParam", columnParam);

            post.setEntity(new StringEntity(payload.toString(), "UTF-8"));
            CloseableHttpResponse response = client.execute(post);
            String result = EntityUtils.toString(response.getEntity(), "UTF-8");
            JSONObject full = JSONUtil.parseObj(result);
            return full.getJSONObject("data");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // 2. 处理指标数据
    static List<Indicator> parseIndicators(JSONObject data) {
        List<Indicator> list = new ArrayList<>();
        int index = 1;
        for (String key : data.keySet()) {
            JSONArray array = data.getJSONObject(key)
                    .getJSONObject("data")
                    .getJSONArray("指标名称");
            for (Object name : array) {
                list.add(new Indicator(index++, "江苏大唐日指标", name.toString()));
            }
        }
        return list;
    }

    // 3. 创建索引
    static void createIndexIfNotExists(ElasticsearchClient client) throws IOException {
        boolean exists = client.indices().exists(e -> e.index(INDEX_NAME)).value();
        if (exists) {
            System.out.println("⚠️ 索引已存在，跳过创建");
            return;
        }

        client.indices().create(c -> c
                .index(INDEX_NAME)
                .settings(s -> s
                        .numberOfShards("1")
                        .analysis(a -> a
                                .analyzer("ik_max_word_analyzer", an -> an
                                        .custom(ca -> ca.tokenizer("ik_max_word")))))
                .mappings(m -> m
                        .properties("title", p -> p.text(t -> t.analyzer("ik_max_word").searchAnalyzer("ik_smart")))
                        .properties("content", p -> p.text(t -> t.analyzer("ik_max_word").searchAnalyzer("ik_smart")))
                        .properties("index", p -> p.integer(i -> i))
                        .properties("embedding", p -> p
                                .denseVector(d -> d.dims(1024).index(true).similarity("cosine"))))
        );

        System.out.println("✅ 已创建索引: " + INDEX_NAME);
    }

    // 4. 嵌入 + 写入 ES
    static void bulkInsertToES(ElasticsearchClient client, List<Indicator> indicators) throws IOException {
        List<BulkOperation> operations = new ArrayList<>();

        for (Indicator indicator : indicators) {
            List<Float> embedding = getEmbedding(indicator.content);

            Map<String, Object> doc = new HashMap<>();
            doc.put("title", indicator.title);
            doc.put("content", indicator.content);
            doc.put("index", indicator.index);
            doc.put("embedding", embedding);

            operations.add(new BulkOperation.Builder()
                    .index(i -> i.index(INDEX_NAME).document(doc))
                    .build());
        }

        BulkRequest request = new BulkRequest.Builder().operations(operations).build();
        BulkResponse response = client.bulk(request);
        System.out.println("✅ 成功写入文档数：" + response.items().size());
    }

    // 5. 获取向量 embedding（使用 OpenAI 兼容服务）
    static List<Float> getEmbedding(String text) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(OPENAI_EMBEDDING_URL);
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Authorization", "Bearer " + OPENAI_API_KEY);

            JSONObject payload = new JSONObject();
            payload.set("input", text);
            payload.set("model", "bge-large-zh-v1.5");

            post.setEntity(new StringEntity(payload.toString(), "UTF-8"));
            CloseableHttpResponse response = client.execute(post);
            String result = EntityUtils.toString(response.getEntity(), "UTF-8");

            JSONObject json = JSONUtil.parseObj(result);
            JSONArray embeddingArray = json.getJSONArray("data")
                    .getJSONObject(0)
                    .getJSONArray("embedding");

            List<Float> vector = new ArrayList<>();
            for (Object num : embeddingArray) {
                vector.add(Float.parseFloat(num.toString()));
            }
            return vector;
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    // 指标类
    static class Indicator {
        int index;
        String title;
        String content;

        Indicator(int index, String title, String content) {
            this.index = index;
            this.title = title;
            this.content = content;
        }
    }
}
