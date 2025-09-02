package com.example.service;

import cn.hutool.core.util.StrUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.example.util.EmbeddingUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.example.dto.SearchResponseDto;

import java.io.IOException;
import java.util.*;

@Service
public class EsSearchWithEmbeddedService {

    @Value("${external.indicator_es_index}")
    private String INDEX_NAME;

    @Autowired
    private EmbeddingUtil embeddingUtil;

    private final ElasticsearchClient esClient;

    public EsSearchWithEmbeddedService() {
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        this.esClient = new ElasticsearchClient(
                new RestClientTransport(restClient, new JacksonJsonpMapper())
        );
    }

    public List<SearchResponseDto> searchWithEmbedding(String queryText) throws IOException {
        List<Float> queryVector = embeddingUtil.getEmbedding(queryText);
        SearchRequest searchRequest = buildSearchRequest(queryText, queryVector);
        SearchResponse<Map> response = esClient.search(searchRequest, Map.class);

        List<SearchResponseDto> results = new ArrayList<>();
        for (Hit<Map> hit : response.hits().hits()) {
            Map<String, Object> source = hit.source();
            Object indexObj = source.get("index");
            Integer index = (indexObj instanceof Number)
                    ? ((Number) indexObj).intValue()
                    : indexObj != null ? Integer.valueOf(indexObj.toString()) : null;

            results.add(new SearchResponseDto(
                    hit.score(),
                    hit.id(),
                    StrUtil.toStringOrNull(source.get("title")),
                    StrUtil.toStringOrNull(source.get("content"))
            ));
        }
        System.out.println("Results: " + results);
        return results;
    }

    private SearchRequest buildSearchRequest(String queryText, List<Float> queryVector) {
        MultiMatchQuery multiMatch = MultiMatchQuery.of(m -> m
                .query(queryText)
                .fields("title", "content")
                .analyzer("ik_max_word")
        );

        Map<String, JsonData> params = new HashMap<>();
        params.put("query_vector", JsonData.of(queryVector));

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

    private List<SearchResponseDto> extractResults(SearchResponse<Map> response) {
        List<SearchResponseDto> results = new ArrayList<>();

        for (Hit<Map> hit : response.hits().hits()) {
            Map<String, Object> source = hit.source();
            Double score = hit.score();
            String id = hit.id();
            String title = String.valueOf(source.get("title"));
            String content = String.valueOf(source.get("content"));

            results.add(new SearchResponseDto(score,id,title,content));
        }

        return results;
    }

    public List<SearchResponseDto> searchByKeyword(String queryText) throws IOException {
        MultiMatchQuery multiMatch = MultiMatchQuery.of(m -> m
                .query(queryText)
                .fields("title", "content")
                .analyzer("ik_max_word")
        );

        SearchRequest request = SearchRequest.of(s -> s
                .index(INDEX_NAME)
                .query(multiMatch._toQuery())
                .size(10)
        );

        SearchResponse<Map> response = esClient.search(request, Map.class);
        return extractResults(response);
    }

    public List<SearchResponseDto> searchByEmbedding(String queryText) throws IOException {
        List<Float> queryVector = embeddingUtil.getEmbedding(queryText);

        Map<String, JsonData> params = new HashMap<>();
        params.put("query_vector", JsonData.of(queryVector));

        ScriptScoreQuery scriptScore = ScriptScoreQuery.of(s -> s
                .query(q -> q.matchAll(m -> m))
                .script(script -> script
                        .inline(inline -> inline
                                .source("cosineSimilarity(params.query_vector, 'embedding') + 1.0")
                                .params(params)
                        )
                )
        );

        SearchRequest request = SearchRequest.of(s -> s
                .index(INDEX_NAME)
                .query(scriptScore._toQuery())
                .size(10)
        );

        SearchResponse<Map> response = esClient.search(request, Map.class);
        return extractResults(response);
    }

    public List<SearchResponseDto> searchHybridMerged(String queryText) throws IOException {
        List<SearchResponseDto> keywordResults = searchByKeyword(queryText);
        List<SearchResponseDto> embeddingResults = searchByEmbedding(queryText);

        // 使用 index 作为合并依据
        Map<String, SearchResponseDto> mergedMap = new LinkedHashMap<>();

        // 先将关键词结果放入（score * 0.5）
        for (SearchResponseDto dto : keywordResults) {
            double weightedScore = dto.getScore() != null ? dto.getScore() * 0.5 : 0.0;
            mergedMap.put(dto.getId(), new SearchResponseDto(
                    weightedScore,
                    dto.getId(),
                    dto.getTitle(),
                    dto.getContent()
            ));
        }

        // 再处理向量结果（score * 0.5）
        for (SearchResponseDto dto : embeddingResults) {
            double weightedScore = dto.getScore() != null ? dto.getScore() * 0.5 : 0.0;
            if (mergedMap.containsKey(dto.getId())) {
                // 如果已经存在，则将两个score相加
                SearchResponseDto existing = mergedMap.get(dto.getId());
                double newScore = existing.getScore() + weightedScore;
                mergedMap.put(dto.getId(), new SearchResponseDto(
                        newScore,
                        dto.getId(),
                        dto.getTitle(),
                        dto.getContent()
                ));
            } else {
                // 否则直接放入
                mergedMap.put(dto.getId(), new SearchResponseDto(
                        weightedScore,
                        dto.getId(),
                        dto.getTitle(),
                        dto.getContent()
                ));
            }
        }

        // 将合并后的结果按最终得分排序（从高到低）
        List<SearchResponseDto> resultList = new ArrayList<>(mergedMap.values());
        resultList.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));

        return resultList;
    }


}
