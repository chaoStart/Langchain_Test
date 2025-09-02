package com.example.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StorageFinancialData2Es {

    private static final String INDEX_NAME = "suzhou_storage_dataset_big_chunk2";
    private static final int EMBEDDING_DIM = 1024;

    private static ElasticsearchClient createClient() {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ).build();

        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    // 创建索引
// 创建索引
    private static void newClearEs(ElasticsearchClient esClient) throws IOException {
        esClient.indices().create(CreateIndexRequest.of(c -> c
                .index(INDEX_NAME)
                .settings(s -> s.analysis(a -> a
                        .analyzer("ik_max_word_analyzer", aa -> aa
                                .custom(ca -> ca.tokenizer("ik_max_word"))
                        )
                ))
                .mappings(m -> m
                        .properties("company_name", Property.of(p -> p.text(t -> t
                                .analyzer("ik_max_word")
                                .searchAnalyzer("ik_smart")
                                .fields("keyword", f -> f.keyword(k -> k))
                        )))
                        .properties("data_month", Property.of(p -> p.text(t -> t
                                .analyzer("ik_max_word")
                                .searchAnalyzer("ik_smart")
                                .fields("keyword", f -> f.keyword(k -> k))
                        )))
                        .properties("sheet_name", Property.of(p -> p.text(t -> t
                                .analyzer("ik_max_word")
                                .searchAnalyzer("ik_smart")
                                .fields("keyword", f -> f.keyword(k -> k))
                        )))
                        .properties("table_name", Property.of(p -> p.text(t -> t
                                .analyzer("ik_max_word")
                                .searchAnalyzer("ik_smart")
                                .fields("keyword", f -> f.keyword(k -> k))
                        )))
                        .properties("sheet_type", Property.of(p -> p.text(t -> t
                                .analyzer("ik_max_word")
                                .searchAnalyzer("ik_smart")
                        )))
                        .properties("row_name", Property.of(p -> p.text(t -> t
                                .analyzer("ik_max_word")
                                .searchAnalyzer("ik_smart")
                        )))
                        .properties("column_name", Property.of(p -> p.text(t -> t
                                .analyzer("ik_max_word")
                                .searchAnalyzer("ik_smart")
                        )))
                        .properties("embedding", Property.of(p -> p
                                .denseVector(v -> v
                                        .dims(EMBEDDING_DIM)
                                        .index(true)
                                        .similarity("cosine")
                                )
                        ))
                )
        ));
        System.out.println("✅ 已创建索引并启用 IK 分词器: " + INDEX_NAME);
    }

    // 批量存储数据
    public static void save(List<Map<String, Object>> sheetDictList) {
        try {
            ElasticsearchClient client = createClient();

            // 检查索引是否存在
            boolean exists = client.indices().exists(
                    new ExistsRequest.Builder().index(INDEX_NAME).build()
            ).value();
            if (!exists) {
                newClearEs(client);
            } else {
                System.out.println("⚠️ 索引已存在: " + INDEX_NAME);
            }

            BulkRequest.Builder br = new BulkRequest.Builder();

            for (Map<String, Object> item : sheetDictList) {
                String keyType;
                String rowOrColumn;
                if (item.containsKey("row_name") && item.get("row_name") != null) {
                    keyType = "row_name";
                    rowOrColumn = String.valueOf(item.get("row_name"));
                } else {
                    keyType = "column_name";
                    rowOrColumn = String.valueOf(item.get("column_name"));
                }

                // 构造文档
                Map<String, Object> doc = new java.util.HashMap<>();
                doc.put("company_name", String.valueOf(item.get("company_name")));
                doc.put("data_month", String.valueOf(item.get("data_month")));
                doc.put("sheet_name", String.valueOf(item.get("sheet_name")));
                doc.put("table_name", String.valueOf(item.get("table_name")));
                doc.put("sheet_type", String.valueOf(item.get("sheet_type")));
                doc.put("embedding", item.get("embedding"));
                doc.put(keyType, rowOrColumn);

                br.operations(op -> op
                        .index(idx -> idx
                                .index(INDEX_NAME)
                                .document(doc)
                        )
                );
            }

            BulkResponse response = client.bulk(br.build());
            if (response.errors()) {
                System.out.println("⚠️ 批量写入存在错误");
            } else {
                System.out.println("✅ 成功写入 " + sheetDictList.size() + " 条文档到 Elasticsearch 索引：" + INDEX_NAME);
            }
        } catch (Exception e) {
            System.err.println("❌ ES 写入失败: " + e.getMessage());
        }
    }
}
