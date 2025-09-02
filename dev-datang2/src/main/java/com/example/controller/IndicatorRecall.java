package com.example.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IndicatorRecall {

    private static final String ES_HOST = "http://localhost:9200";
    private static final String INDEX_NAME = "datang_docs_index1";
    private static final String EMBEDDING_API_URL = "http://10.3.24.46:9997/v1/embeddings";
    private static final String EMBEDDING_API_KEY = "123";
    private static final Gson gson = new Gson();

    private static ElasticsearchClient createClient() {
        RestClient restClient = RestClient.builder(org.apache.http.HttpHost.create(ES_HOST)).build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    /** Step 0: 调用向量模型接口 */
    private static List<Float> getEmbedding(String text) throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(EMBEDDING_API_URL);
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Authorization", "Bearer " + EMBEDDING_API_KEY);

            JsonObject payload = new JsonObject();
            payload.addProperty("model", "bge-large-zh-v1.5");
            payload.add("input", gson.toJsonTree(text));

            post.setEntity(new StringEntity(payload.toString(), StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(post)) {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                JsonObject json = gson.fromJson(sb.toString(), JsonObject.class);
                JsonArray arr = json.getAsJsonArray("data")
                        .get(0).getAsJsonObject()
                        .getAsJsonArray("embedding");

                List<Float> embedding = new ArrayList<>();
                for (int i = 0; i < arr.size(); i++) {
                    embedding.add(arr.get(i).getAsFloat());
                }
                return embedding;
            }
        }
    }

    /** Step 1: 匹配 company_name */
    private static List<String> searchCompanyName(ElasticsearchClient client, String queryText) throws Exception {
        SearchResponse<Map> response = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(q -> q.match(m -> m.field("company_name").query(queryText)))
                        .source(src -> src.filter(f -> f.includes(Arrays.asList("company_name")))),
                Map.class);

        List<String> companies = new ArrayList<>();
        for (Hit<Map> hit : response.hits().hits()) {
            Map src = hit.source();
            if (src != null && src.containsKey("company_name")) {
                companies.add(src.get("company_name").toString());
            }
        }
        return companies;
    }

    /** Step 3: 公司过滤 + 内容召回 + 向量 rerank */
    private static List<Hit<Map>> searchContent(ElasticsearchClient client,
                                                String queryText,
                                                String companyName,
                                                List<Float> queryVector) throws Exception {

        Script script = new Script.Builder()
                .inline(i -> i
                        .source("cosineSimilarity(params.query_vector, 'embedding') + 1.0")
                        .params(Collections.singletonMap("query_vector",  JsonData.of(queryVector)))
                )
                .build();

        SearchRequest request = new SearchRequest.Builder()
                .index(INDEX_NAME)
                .query(q -> q.scriptScore(ss -> ss
                        .query(innerQ -> innerQ.bool(b -> b
                                .must(m -> m.term(t -> t.field("company_name.keyword").value(companyName)))
                                .should(s -> s.match(m -> m.field("content").query(queryText)))
                        ))
                        .script(script)
                ))
                .source(src -> src.filter(f -> f.includes(Arrays.asList("company_name", "content"))))
                .build();

        SearchResponse<Map> response = client.search(request, Map.class);
        return response.hits().hits();
    }

    /** Step 4: 过滤高分结果 */
    private static List<Hit<Map>> getHighScore(List<Hit<Map>> hits) {
        List<Hit<Map>> result = new ArrayList<>();
        if (hits.isEmpty()) return result;

        double topScore = hits.get(0).score();
        for (Hit<Map> hit : hits) {
            if ((topScore - hit.score()) / topScore <= 0.05) {
                result.add(hit);
            }
        }
        return result;
    }

    /** Step 5: 合并相同公司下的内容 */
    private static Map<String, List<String>> mergeByCompany(List<Hit<Map>> hits) {
        Map<String, List<String>> results = new HashMap<>();
        for (Hit<Map> hit : hits) {
            Map src = hit.source();
            if (src == null) continue;
            String company = src.get("company_name").toString();
            String content = src.get("content").toString();
            results.computeIfAbsent(company, k -> new ArrayList<>()).add(content);
        }
        return results;
    }

    public static void main(String[] args) throws Exception {
        ElasticsearchClient client = createClient();

        String queryText = "大唐集团苏州分公司的发电量是多少？";

        // Step 1: 查找公司名
        List<String> companies = searchCompanyName(client, queryText);
        String companyName;
        if (companies.isEmpty()) {
            System.out.println("未匹配到公司名称，使用默认公司");
            companyName = "大唐集团苏州分公司";
        } else {
            companyName = companies.get(0);
        }
        System.out.println("✅ 匹配到公司：" + companyName);

        // Step 2: 获取向量
        List<Float> queryVector = getEmbedding(queryText);

        // Step 3: 召回+rerank
        List<Hit<Map>> results = searchContent(client, queryText, companyName, queryVector);

        // Step 4: 高分筛选
        List<Hit<Map>> highScoreHits = getHighScore(results);

        // Step 5: 合并
        Map<String, List<String>> merged = mergeByCompany(highScoreHits);

        for (Map.Entry<String, List<String>> entry : merged.entrySet()) {
            System.out.println("公司名称: " + entry.getKey() + " - 日指标名称: " + entry.getValue());
        }
    }
}
