package com.example.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class SheetSearchService {

    @Value("${elasticsearch.index}")
    private String indexName;
//    // 等价于下面的使用@Autowired进行注入
//    private final ElasticsearchClient esClient;
//    public SheetSearchService(ElasticsearchClient esClient) {
//        this.esClient = esClient;
//    }
    @Autowired
    private ElasticsearchClient esClient;

    public List<String> searchSheetNames(String keywords, int topK) throws IOException {
        List<Query> shouldQueries = Arrays.asList(
                MatchQuery.of(m -> m.field("row_name").query(keywords).boost(4.0f).analyzer("ik_max_word"))._toQuery(),
                MatchQuery.of(m -> m.field("column_name").query(keywords).boost(1.0f).analyzer("ik_max_word"))._toQuery(),
                MatchQuery.of(m -> m.field("sheet_name").query(keywords).boost(2.0f).analyzer("ik_max_word"))._toQuery()
        );

        Query boolQuery = BoolQuery.of(b -> b.should(shouldQueries).minimumShouldMatch("1"))._toQuery();

        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .query(boolQuery)
                .source(src -> src.filter(f -> f.includes("sheet_name")))
                .size(topK)
        );

        SearchResponse<Map> response = esClient.search(request, Map.class);

        List<String> sheetNames = new ArrayList<>();
        for (Hit<Map> hit : response.hits().hits()) {
            Object sheetName = hit.source().get("sheet_name");
            if (sheetName != null) {
                sheetNames.add(sheetName.toString());
            }
        }
        return sheetNames;
    }

    public long countDocuments() throws IOException {
        CountRequest request = CountRequest.of(c -> c.index(indexName));
        CountResponse response = esClient.count(request);
        return response.count();
    }
}

