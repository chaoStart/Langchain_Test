package com.example.controller;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.client.RestClient;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class RecallHybrid {

    private static final String ES_URL = "http://localhost:9200";
    private static final String INDEX_NAME = "datang_docs_index1";

    private static final String EMBEDDING_API_URL = "http://10.3.24.46:9997/v1/embeddings";
    private static final String EMBEDDING_API_KEY = "123";

    private static final Gson gson = new Gson();

    private static ElasticsearchClient esClient;

    static {
        RestClient restClient = RestClient.builder(HttpHost.create(ES_URL)).build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        esClient = new ElasticsearchClient(transport);
    }

    // 调用 Embedding API
    public static List<Float> getEmbedding(String text) throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(EMBEDDING_API_URL);
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Authorization", "Bearer " + EMBEDDING_API_KEY);

            JsonObject body = new JsonObject();
            body.addProperty("model", "bge-large-zh-v1.5");
            body.add("input", gson.toJsonTree(text));

            post.setEntity(new StringEntity(body.toString(), StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(post)) {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
                String result = reader.lines().collect(Collectors.joining());
                JsonObject json = gson.fromJson(result, JsonObject.class);

                JsonArray arr = json.getAsJsonArray("data")
                        .get(0).getAsJsonObject()
                        .getAsJsonArray("embedding");

                List<Float> embedding = new ArrayList<>();
                arr.forEach(e -> embedding.add(e.getAsFloat()));
                System.out.println("嵌入的查询文本: " + text);
                return embedding;
            }
        }
    }

    // 混合搜索
    public static List<Map<String, Object>> hybridSearchIndicator(
            String queryText, List<Float> queryVector, String companyName) throws Exception {

        // ===== 1. 关键词检索 =====
        Query keywordQuery = BoolQuery.of(b -> b
                .must(TermQuery.of(t -> t.field("company_name.keyword").value(companyName))._toQuery())
                .must(MatchQuery.of(m -> m.field("content").query(queryText).analyzer("ik_max_word"))._toQuery())
        )._toQuery();

        SearchResponse<Map> keywordRes = esClient.search(s -> s
                .index(INDEX_NAME)
                .query(keywordQuery)
                .size(10), Map.class);

        // ===== 2. 向量检索 =====
        Map<String, JsonData> params = new HashMap<>();
        params.put("query_vector", JsonData.of(queryVector));

        Query vectorQuery = ScriptScoreQuery.of(s -> s
                .query(BoolQuery.of(b -> b
                        .must(TermQuery.of(t -> t.field("company_name.keyword").value(companyName))._toQuery())
                        .must(ExistsQuery.of(e -> e.field("content"))._toQuery())
                )._toQuery())
                .script(script -> script
                        .inline(inline -> inline
                                .source("cosineSimilarity(params.query_vector, 'embedding') + 1.0")
                                .params(params)
                        )
                )
        )._toQuery();

        SearchResponse<Map> vectorRes = esClient.search(s -> s
                .index(INDEX_NAME)
                .query(vectorQuery)
                .size(10), Map.class);

        // ===== 3. 合并逻辑 =====
        Map<String, Map<String, Object>> merged = new HashMap<>();

        for (Hit<Map> hit : keywordRes.hits().hits()) {
            Map<String, Object> src = hit.source();
            String key = src.get("company_name") + "|" + src.get("dataset_id") + "|" + src.get("content");
            Map<String, Object> val = new HashMap<>();
            // 获取最大数据

            val.put("source", src);
            val.put("bm25_score", hit.score() != null ? hit.score() : 0.0);
            val.put("vector_score", 0.0);
            merged.put(key, val);
        }

        for (Hit<Map> hit : vectorRes.hits().hits()) {
            Map<String, Object> src = hit.source();
            String key = src.get("company_name") + "|" + src.get("dataset_id") + "|" + src.get("content");
            Map<String, Object> val = merged.getOrDefault(key, new HashMap<>());
            if (!val.containsKey("source")) val.put("source", src);
            val.put("bm25_score", val.getOrDefault("bm25_score", 0.0));
            val.put("vector_score", hit.score() != null ? hit.score() : 0.0);
            merged.put(key, val);
        }

        // 计算新得分
        List<Map<String, Object>> finalResults = new ArrayList<>();
        for (Map<String, Object> val : merged.values()) {
            double bm25 = (double) val.get("bm25_score");
            double vec = (double) val.get("vector_score");
            double score;
            if (bm25 > 0 && vec > 0) {
                score = bm25 * 0.5 + vec * 0.5;
            } else {
                score = Math.max(bm25, vec) * 0.5;
            }
            Map<String, Object> src = (Map<String, Object>) val.get("source");
            Map<String, Object> item = new HashMap<>();
            item.put("score", score);
            item.put("company_name", src.get("company_name"));
            item.put("dataset_id", src.get("dataset_id"));
            item.put("content", src.get("content"));
            finalResults.add(item);
        }

        finalResults.sort((a, b) -> Double.compare((double) b.get("score"), (double) a.get("score")));
        System.out.println("关键词+向量召回的指标数据总数:" + finalResults.size());
        return finalResults.stream().limit(10).collect(Collectors.toList());
    }

    // 过滤高分数据
    public static List<Map<String, Object>> getHighScore(List<Map<String, Object>> results) {
        List<Map<String, Object>> high = new ArrayList<>();
        if (results.isEmpty()) return high;
        double score1 = (double) results.get(0).get("score");
        for (Map<String, Object> item : results) {
            double score = (double) item.get("score");
            if ((score1 - score) / score1 <= 0.3) {
                high.add(item);
            }
        }
        return high;
    }

    // 归一化数据为0~1之间
    public static Float normalizeScore(double score) {


        return (float) (score - 0.5) * 2;
    }

    // 合并相同公司内容
    public static Map<String, List<Map<String, List<String>>>> sameCompanyContentMerged(
            List<Map<String, Object>> data) {

        Map<String, List<Map<String, List<String>>>> results = new HashMap<>();

        for (Map<String, Object> d : data) {
            String company = (String) d.get("company_name");
            String datasetId = (String) d.get("dataset_id");
            String content = (String) d.get("content");

            results.putIfAbsent(company, new ArrayList<>());

            boolean found = false;
            for (Map<String, List<String>> item : results.get(company)) {
                if (item.containsKey(datasetId)) {
                    item.get(datasetId).add(content);
                    found = true;
                    break;
                }
            }
            if (!found) {
                Map<String, List<String>> newEntry = new HashMap<>();
                newEntry.put(datasetId, new ArrayList<>(Collections.singletonList(content)));
                results.get(company).add(newEntry);
            }
        }
        return results;
    }

    public static void main(String[] args) throws Exception {
        String queryText = "公司名称:大唐江苏有限公司 时间:六月份 指标名称:燃机汇总上网电量月指标（大屏）是多少";

        List<Float> queryVector = getEmbedding(queryText);

        List<Map<String, Object>> results = hybridSearchIndicator(queryText, queryVector, "大唐江苏有限公司");

        List<Map<String, Object>> highIndicatorRes = getHighScore(results);

        Map<String, List<Map<String, List<String>>>> mergedRes = sameCompanyContentMerged(highIndicatorRes);

        System.out.println("查询的指标数据:\n" + gson.toJson(mergedRes));

        for (Map.Entry<String, List<Map<String, List<String>>>> entry : mergedRes.entrySet()) {
            String company = entry.getKey();
            for (Map<String, List<String>> item : entry.getValue()) {
                for (Map.Entry<String, List<String>> dt : item.entrySet()) {
                    System.out.println("公司名称: " + company + " - 数据集ID:" + dt.getKey() + " - 对应的指标名称:" + dt.getValue());
                }
            }
        }
    }
}

