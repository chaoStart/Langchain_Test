package com.example.service;

import cn.hutool.json.*;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.example.dto.IndicatorRequest;
import com.example.entity.Indicator;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.example.util.EmbeddingUtil;
import java.io.IOException;
import java.util.*;

@Service
public class DatangIndexService {

    @Value("${openai.api.url}")
    private String embeddingApiUrl;

    @Value("${openai.api.key}")
    private String embeddingApiKey;

    @Value("${external.indicator_url}")
    private String indicator_url;

    @Value("${external.indicator_es_index}")
    private  String indicator_es_index;

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private EmbeddingUtil embeddingUtil;
    public void index(IndicatorRequest req) throws IOException {
        JSONObject responseData = fetchDataFromApi(req);
        if (responseData == null) {
            System.err.println("❌ 请求失败，程序退出");
            return;
        }

        List<Indicator> indicators = parseIndicators(responseData);
        System.out.println("📌 提取指标数量：" + indicators.size());

        createIndexIfNotExists();
        bulkInsertToES(indicators);
    }

    private JSONObject fetchDataFromApi(IndicatorRequest req) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(indicator_url);
            post.setHeader("Content-Type", "application/json");

            JSONObject payload = new JSONObject();
            payload.set("code", "");
            payload.set("name", "");
            payload.set("companyName",req.getCompanyName());
            payload.set("queryType", "0");
            payload.set("starttime",req.getStartTime());
            payload.set("endtime", req.getEndTime());
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

    private List<Indicator> parseIndicators(JSONObject data) {
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

    private void createIndexIfNotExists() throws IOException {
        String indexName = indicator_es_index;
        boolean exists = esClient.indices().exists(e -> e.index(indexName)).value();
        if (exists) {
            System.out.println("⚠️ 索引已存在，跳过创建");
            return;
        }

        esClient.indices().create(c -> c
                .index(indexName)
                .settings(s -> s
                        .numberOfShards("1")
                        .analysis(a -> a
                                .analyzer("ik_max_word_analyzer", an -> an
                                        .custom(ca -> ca.tokenizer("ik_max_word")))))
                .mappings(m -> m
                        .properties("title", p -> p.text(t -> t.analyzer("ik_max_word").searchAnalyzer("ik_smart")))
                        .properties("content", p -> p.text(t -> t.analyzer("ik_max_word").searchAnalyzer("ik_smart")))
                        .properties("index", p -> p.integer(i -> i))
                        .properties("embedding", p -> p.denseVector(d -> d.dims(1024).index(true).similarity("cosine"))))
        );

        System.out.println("✅ 已创建索引: " + indexName);
    }

    private void bulkInsertToES(List<Indicator> indicators) throws IOException {
        List<BulkOperation> operations = new ArrayList<>();
        for (Indicator indicator : indicators) {
            List<Float> embedding = embeddingUtil.getEmbedding(indicator.getContent());

            Map<String, Object> doc = new HashMap<>();
            doc.put("title", indicator.getTitle());
            doc.put("content", indicator.getContent());
            doc.put("index", indicator.getIndex());
            doc.put("embedding", embedding);

            operations.add(new BulkOperation.Builder()
                    .index(i -> i.index(indicator_es_index).document(doc))
                    .build());
        }

        BulkRequest request = new BulkRequest.Builder().operations(operations).build();
        BulkResponse response = esClient.bulk(request);
        System.out.println("✅ 成功写入文档数：" + response.items().size());
    }
}

