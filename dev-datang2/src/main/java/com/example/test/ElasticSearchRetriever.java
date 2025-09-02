package com.example.test;
import com.example.util.PromptBuilder;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.gson.Gson;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchRetriever {

    public static void main(String[] args) throws IOException {
        // Step 1: 构建 ES 客户端
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        ElasticsearchClient esClient = new ElasticsearchClient(
                new RestClientTransport(restClient, new JacksonJsonpMapper()));

        String indexName = "datang_docs_index1";
        String userQuery = "徐塘公司发电收益年指标";

        // Step 2: 查询 ES
        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .size(10)
                .query(q -> q
                        .multiMatch(m -> m
                                .query(userQuery)
                                .fields("title^1", "content^3")
                                .analyzer("ik_max_word")
                        )
                )
        );

        SearchResponse<Map> response = esClient.search(request, Map.class);
        System.out.println("查询结果 Top 10："+response);

        Map<String, Map<String, Object>> relevantIndex = new HashMap<>();

        for (Hit<Map> hit : response.hits().hits()) {
            Map source = hit.source();
            String idx = String.valueOf(source.get("index"));
            String title = String.valueOf(source.get("title"));
            String content = String.valueOf(source.get("content"));
            double score = hit.score();

            Map<String, Object> doc = new HashMap<>();
            doc.put("index", idx);
            doc.put("title", title);
            doc.put("content", content);
            doc.put("score", score);

            relevantIndex.put(idx, doc);

            System.out.println("- Index: " + idx);
            System.out.println("  Title: " + title);
            System.out.println("  Content: " + content);
            System.out.println("  Score: " + score);
        }

        System.out.println("\nES数据库中检索到的所有与用户相关的index指标名称:");
        System.out.println(new Gson().toJson(relevantIndex));

        // Step 3: 构造 prompt
        String prompt = PromptBuilder.buildChooseIndexPrompt(relevantIndex, userQuery);

        // Step 4: 调用大模型
        OpenAiChatModel model = OpenAiChatModel.builder()
                .apiKey("sk-47c855b5e7114a02854ee2a63e472b1e") // 替换为你自己的 key
                .baseUrl("https://dashscope.aliyuncs.com/compatible-mode/v1/")
                .modelName("qwen-max")
                .build();

        String result = model.generate(prompt);

        System.out.println("\n调用大模型获取到的最相关的索引及对应的指标名称:");
        System.out.println(result);

        // 关闭 ES 客户端
        restClient.close();
    }
}
