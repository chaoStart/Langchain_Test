package com.example.test;

import cn.hutool.core.util.StrUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import co.elastic.clients.json.JsonData;
import java.io.IOException;
import java.util.*;

public class EsSearchWithEmbedding {

    private static final String INDEX_NAME = "datang_docs_index3";
    private static final String OPENAI_BASE_URL = "http://10.3.24.46:9997/v1";
    private static final String OPENAI_API_KEY = "123";

    public static void main(String[] args) throws IOException {
        // 1. 初始化 Elasticsearch 客户端
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        ElasticsearchClient esClient = new ElasticsearchClient(
                new RestClientTransport(restClient, new JacksonJsonpMapper())
        );

        // 2. 获取文本嵌入向量
        String queryText = "煤机完成率";
        List<Float> queryVector = getEmbedding(queryText);

        // 3. 构建查询
        SearchRequest searchRequest = buildSearchRequest(queryText, queryVector);

        // 4. 执行查询
        SearchResponse<Map> response = esClient.search(searchRequest, Map.class);

        // 5. 打印结果
        for (Hit<Map> hit : response.hits().hits()) {
            Map<String, Object> source = hit.source();
            Double score = hit.score();
            String title = StrUtil.toStringOrNull(source.get("title"));
            String content = StrUtil.toStringOrNull(source.get("content"));
            System.out.printf("[%.2f] %s - %s%n", score, title, content);
        }

        restClient.close();
    }

    private static List<Float> getEmbedding(String text) {
        OpenAiEmbeddingModel model = OpenAiEmbeddingModel.builder()
                .baseUrl(OPENAI_BASE_URL)
                .apiKey(OPENAI_API_KEY)
                .modelName("bge-large-zh-v1.5")
                .build();

        Embedding embedding = model.embed(text).content(); // 返回 Embedding 对象，内部是 List<Float>
        return embedding.vectorAsList(); // 获取 float 向量
    }

    private static SearchRequest buildSearchRequest(String queryText, List<Float> queryVector) {
        // 文本匹配查询
        MultiMatchQuery multiMatch = MultiMatchQuery.of(m -> m
                .query(queryText)
                .fields("title", "content")
                .analyzer("ik_max_word")
        );

        // 构造 script_score 查询，注意：Elasticsearch Java SDK 使用 inline 脚本
        Map<String, JsonData> params = new HashMap<>();
        params.put("query_vector",JsonData.of(queryVector));

        ScriptScoreQuery scriptScore = ScriptScoreQuery.of(s -> s
                .query(q -> q.matchAll(m -> m))
                .script(script -> script
                        .inline(inline -> inline
                                .source("cosineSimilarity(params.query_vector, 'embedding') + 1.0")
                                .params(params)
                        )
                )
        );

        BoolQuery boolQuery = BoolQuery.of(b -> b
                .must(multiMatch._toQuery())
                .should(scriptScore._toQuery())
        );

        return SearchRequest.of(s -> s
                .index(INDEX_NAME)
                .query(boolQuery._toQuery())
                .size(10)
        );
    }
}
