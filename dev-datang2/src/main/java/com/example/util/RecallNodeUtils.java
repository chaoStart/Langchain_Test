package com.example.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.ScriptScoreQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class RecallNodeUtils {

    private ElasticsearchClient esClient;
    private String indexName = "suzhou_storage_dataset_big_chunk1";

    public RecallNodeUtils(ElasticsearchClient esClient) {
        this.esClient = esClient;
    }

    public List<Map<String, Object>> queryByRowTextEmbedded(String queryText, List<Float> queryVector) throws IOException {
        List<Map<String, Object>> finalResults = new ArrayList<>();

        // 1. 关键词查询
        Query keywordQuery = Query.of(q -> q
                .bool(b -> b
                        .must(m -> m.exists(e -> e.field("row_name")))
                        .should(s -> s.match(m -> m.field("row_name").query(queryText).analyzer("ik_max_word").boost(1.0F)))
                        .should(s -> s.match(m -> m.field("table_name").query(queryText).analyzer("ik_max_word").boost(2.0F)))
                        .minimumShouldMatch("1")
                )
        );

        SearchResponse<Map> keywordRes = esClient.search(s -> s
                        .index(indexName)
                        .query(keywordQuery)
                        .size(50),
                Map.class
        );

        // 2. 向量查询
        Map<String, JsonData> params = new HashMap<>();
        params.put("query_vector", JsonData.of(queryVector));

        ScriptScoreQuery scriptScore = ScriptScoreQuery.of(s -> s
                .query(q -> q.bool(b -> b
                        .must(m -> m.exists(e -> e.field("row_name"))) // ✅ 限制必须有 row_name
                ))
                .script(script -> script
                        .inline(inline -> inline
                                .source("cosineSimilarity(params.query_vector, 'embedding') + 1.0")
                                .params(params)
                        )
                )
        );

        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .query(scriptScore._toQuery())
                .size(50)
        );

        SearchResponse<Map> vectorRes = esClient.search(request, Map.class);

        // 3. 合并逻辑
        Map<String, Map<String, Object>> merged = new HashMap<>();

        for (Hit<Map> hit : keywordRes.hits().hits()) {
            Map<String, Object> source = hit.source();
            String key = source.get("table_name") + "|" + source.get("row_name");
            Map<String, Object> val = new HashMap<>();
            val.put("source", source);
            val.put("bm25_score", hit.score());
            val.put("vector_score", 0.0);
            merged.put(key, val);
        }

        for (Hit<Map> hit : vectorRes.hits().hits()) {
            Map<String, Object> source = hit.source();
            String content = source.containsKey("row_name") ? (String) source.get("row_name") : (String) source.get("column_name");
            String key = source.get("table_name") + "|" + content;

            if (merged.containsKey(key)) {
                merged.get(key).put("vector_score", hit.score());
            } else {
                Map<String, Object> val = new HashMap<>();
                val.put("source", source);
                val.put("bm25_score", 0.0);
                val.put("vector_score", hit.score());
                merged.put(key, val);
            }
        }

        for (Map<String, Object> val : merged.values()) {
            Map<String, Object> source = (Map<String, Object>) val.get("source");
            double bm25 = (double) val.get("bm25_score");
            double vec = (double) val.get("vector_score");
            double score = (bm25 > 0 && vec > 0) ? (bm25 + vec) * 0.5 : Math.max(bm25, vec) * 0.5;

            Map<String, Object> result = new HashMap<>();
            result.put("score", score);
            result.put("company_name", source.get("company_name"));
            result.put("sheet_name", source.get("sheet_name"));
            result.put("table_name", source.get("table_name"));
            result.put("data_month", source.get("data_month"));
            result.put("sheet_type", source.getOrDefault("sheet_type", ""));
            result.put("row_name", source.get("row_name"));
            result.put("column_name", source.get("column_name"));
            finalResults.add(result);
        }

        finalResults.sort((a, b) -> Double.compare((double) b.get("score"), (double) a.get("score")));
        return finalResults.subList(0, Math.min(10, finalResults.size()));
    }

    public List<Map<String, Object>> queryByColumnTextEmbedded(String queryText, List<Float> queryVector, Map<String, Object> filterConditions) throws IOException {
        List<Map<String, Object>> finalResults = new ArrayList<>();
        Map<String, Map<String, Object>> merged = new HashMap<>();

        // 1. 关键词检索
        Query keywordQuery = Query.of(q -> q.bool(b -> b
                .must(m -> m.term(t -> t.field("company_name").value((String) filterConditions.get("company_name"))))
                .must(m -> m.term(t -> t.field("table_name.keyword").value((String) filterConditions.get("table_name"))))
                .must(m -> m.term(t -> t.field("data_month").value((String) filterConditions.get("data_month"))))
                .must(m -> m.exists(e -> e.field("column_name")))
                .should(s -> s.match(m -> m.field("column_name").query(queryText).analyzer("ik_max_word")))
                .minimumShouldMatch("1")
        ));

        SearchResponse<Map> keywordRes = esClient.search(s -> s.index(indexName).query(keywordQuery).size(50), Map.class);
        // 检查是否命中，如果列名称没有命中则返回空结果
        if (keywordRes.hits().hits().isEmpty()) {
            return Collections.emptyList(); // 返回空的 List<Map<String, Object>>
        }
        // 2. 向量检索
        Map<String, JsonData> params = new HashMap<>();
        params.put("query_vector", JsonData.of(queryVector));

        ScriptScoreQuery scriptScore = ScriptScoreQuery.of(s -> s.query(q -> q.bool(b -> b
                // ✅ 限制必须匹配 company_name
                .must(m -> m.term(t -> t.field("company_name").value((String) filterConditions.get("company_name"))))
                // ✅ 限制必须匹配 table_name
                .must(m -> m.term(t -> t.field("table_name.keyword").value((String) filterConditions.get("table_name"))))
                // ✅ 限制必须匹配 data_month
                .must(m -> m.term(t -> t.field("data_month").value((String) filterConditions.get("data_month"))))
                // ✅ 限制必须有 column_name
                .must(m -> m.exists(e -> e.field("column_name")))
                ))
                .script(script -> script
                        .inline(inline -> inline
                                .source("cosineSimilarity(params.query_vector, 'embedding') + 1.0")
                                .params(params)
                        )
                )
        );
        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .query(scriptScore._toQuery())
                .size(50)
        );

        SearchResponse<Map> vectorRes = esClient.search(request, Map.class);

        // 合并逻辑
        for (Hit<Map> hit : keywordRes.hits().hits()) {
            Map<String, Object> source = hit.source();
            String key = source.get("table_name") + "|" + source.get("column_name");
            Map<String, Object> val = new HashMap<>();
            val.put("source", source);
            val.put("bm25_score", hit.score());
            val.put("vector_score", 0.0);
            merged.put(key, val);
        }

        for (Hit<Map> hit : vectorRes.hits().hits()) {
            Map<String, Object> source = hit.source();
            String key = source.get("table_name") + "|" + source.get("column_name");
            if (merged.containsKey(key)) {
                merged.get(key).put("vector_score", hit.score());
            } else {
                Map<String, Object> val = new HashMap<>();
                val.put("source", source);
                val.put("bm25_score", 0.0);
                val.put("vector_score", hit.score());
                merged.put(key, val);
            }
        }

        for (Map<String, Object> val : merged.values()) {
            Map<String, Object> source = (Map<String, Object>) val.get("source");
            double bm25 = (double) val.get("bm25_score");
            double vec = (double) val.get("vector_score");
            double score = (bm25 > 0 && vec > 0) ? (bm25 + vec) * 0.5 : Math.max(bm25, vec) * 0.5;

            Map<String, Object> result = new HashMap<>();
            result.put("score", score);
            result.put("company_name", source.get("company_name"));
            result.put("sheet_name", source.get("sheet_name"));
            result.put("table_name", source.get("table_name"));
            result.put("data_month", source.get("data_month"));
            result.put("sheet_type", source.getOrDefault("sheet_type", ""));
            result.put("column_name", source.get("column_name"));
            finalResults.add(result);
        }

        finalResults.sort((a, b) -> Double.compare((double) b.get("score"), (double) a.get("score")));
        return finalResults;
    }

    public String getAllColumnName(Map<String, Object> filterConditions) throws IOException {
        Query query = Query.of(q -> q.bool(b -> b
                .must(m -> m.term(t -> t.field("company_name").value((String) filterConditions.get("company_name"))))
                .must(m -> m.term(t -> t.field("table_name.keyword").value((String) filterConditions.get("table_name"))))
                .must(m -> m.term(t -> t.field("data_month").value((String) filterConditions.get("data_month"))))
        ));

        SearchResponse<Map> response = esClient.search(s -> s.index(indexName).query(query).size(1000), Map.class);

        Map<String, List<String>> tableColumns = new HashMap<>();
        for (Hit<Map> hit : response.hits().hits()) {
            Map<String, Object> doc = hit.source();
            String sheetName = (String) doc.get("sheet_name");
            String columnName = doc.containsKey("column_name") ? (String) doc.get("column_name") : "";
            if (!columnName.isEmpty()) {
                tableColumns.computeIfAbsent(sheetName, k -> new ArrayList<>()).add(columnName);
            }
        }

        Map<String, String> tableColumnsStr = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : tableColumns.entrySet()) {
            tableColumnsStr.put(entry.getKey(), String.join("\n", entry.getValue()));
        }

        StringBuilder columnsStr = new StringBuilder();
        for (String cols : tableColumnsStr.values()) {
            columnsStr.append(cols).append("\n");
        }

        return columnsStr.toString().trim();
    }

    public List<String> getSheetNameUnit(Map<String, Object> filterConditions) {
        try {
            // 构建查询条件
            Query query = Query.of(q -> q.bool(b -> b
                    .must(m -> m.term(t -> t.field("company_name").value(filterConditions.get("company_name").toString())))
                    .must(m -> m.term(t -> t.field("table_name.keyword").value(filterConditions.get("table_name").toString())))
                    .must(m -> m.term(t -> t.field("data_month").value(filterConditions.get("data_month").toString())))
                    .must(m -> m.exists(e -> e.field("column_name")))
            ));

            // 执行查询
            SearchResponse<Map> response = esClient.search(s -> s
                            .index(indexName)
                            .query(query)
                            .size(50),
                    Map.class
            );

            // 遍历结果
            List<Hit<Map>> hits = response.hits().hits();
            for (Hit<Map> hit : hits) {
                Map<String, Object> doc = hit.source();
                if (doc != null && "单位".equals(doc.get("column_name"))) {
                    // 找到单位后返回 List
                    return Collections.singletonList("单位");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        // 没有找到返回空列表
        return Collections.emptyList();
    }
}


