package com.example.controller;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.example.util.RecallNodeUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;

public class RecallNodeRowColumn {

    //初始化主类
    public static void main(String[] args) throws IOException {
        // 1. 初始化 Elasticsearch 客户端
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        ElasticsearchClient esClient = new ElasticsearchClient(
                new RestClientTransport(restClient, new JacksonJsonpMapper())
        );

        // 2. 初始化 RecallNodeRowColumn
        RecallNodeRowColumn recall = new RecallNodeRowColumn(esClient);

        // 3. 设置查询问题
//        String queryText = "利润表中基本每股收益的上年同期累计、本月金额和本年累计是多少?";
//        String queryText = "8月的利润明细表中保险服务收入本年累计是多少?";
        String queryText = "8月的调整后资本是多少";
        // 假设已经有 embedding 向量
        List<Float> queryVector = getEmbedding(queryText);

        // 4. 调用召回方法
        List<Map<String, Object>> completeRowColumn = recall.runRecall(queryText, queryVector);

        // 5. 打印输出
        System.out.println("查询问题: " + queryText + "\n");
        for (Map<String, Object> data : completeRowColumn) {
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                String sheetName = entry.getKey();
                Map<String, Object> v = (Map<String, Object>) entry.getValue();
                System.out.println(String.format(
                        "sheet_name:%s -- company_name:%s -- data_month:%s\nrow_list:%s\ncolumn_list:%s\n",
                        v.get("sheet_name"),
                        v.get("company_name"),
                        v.get("data_month"),
                        v.get("row_list"),
                        v.get("column_list")
                ));
            }
        }
    }

    //初始化嵌入向量
    public static List<Float> getEmbedding(String text) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost("http://10.3.24.46:9997/v1/embeddings");
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Authorization", "Bearer " + "123");

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

    private RecallNodeUtils recallnodeutils;

    public RecallNodeRowColumn(ElasticsearchClient esClient) {
        this.recallnodeutils = new RecallNodeUtils(esClient);
    }

    public Map<String, Map<String, Object>> sameSheetContentMerged(List<Map<String, List<String>>> mergeList) {
        Map<String, List<String>> merged = new HashMap<>();
        for (Map<String, List<String>> d : mergeList) {
            for (Map.Entry<String, List<String>> entry : d.entrySet()) {
                merged.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
            }
        }
        Map<String, Map<String, Object>> result = new HashMap<>();
        for (Map.Entry<String, List<String>> e : merged.entrySet()) {
            Map<String, Object> map = new HashMap<>();
            map.put("list", e.getValue());
            result.put(e.getKey(), map);
        }
        return result;
    }

    // 高分行筛选
    public List<Map<String, Object>> getHighResults(List<Map<String, Object>> resultsByHybrid) {
        List<Map<String, Object>> recordHighScore = new ArrayList<>();
        if (!resultsByHybrid.isEmpty()) {
            double score1 = (double) resultsByHybrid.get(0).get("score");
            for (Map<String, Object> item : resultsByHybrid) {
                double scoreX = (double) item.get("score");
                if ((score1 - scoreX) / score1 <= 0.2) {
                    recordHighScore.add(item);
                }
            }
        }
        return recordHighScore;
    }

    public List<Map<String, Object>> runRecall(String queryText, List<Float> queryVector) throws IOException {
        List<Map<String, Object>> rowResults = recallnodeutils.queryByRowTextEmbedded(queryText, queryVector);
        List<Map<String, Object>> recordHighRowScore = getHighResults(rowResults);

        // 生成 location_sheet_info
        Map<String, Map<String, Object>> locationSheetInfo = new HashMap<>();
        for (Map<String, Object> v : recordHighRowScore) {
            String tableName = (String) v.get("table_name");
            if (!locationSheetInfo.containsKey(tableName)) {
                Map<String, Object> info = new HashMap<>();
                info.put("company_name", v.get("company_name"));
                info.put("sheet_name", v.get("sheet_name"));
                info.put("table_name", tableName);
                info.put("data_month", v.get("data_month"));
                locationSheetInfo.put(tableName, info);
            }
        }

        // 合并行数据
        List<Map<String, List<String>>> mergeAllRowList = new ArrayList<>();
        for (Map<String, Object> v : locationSheetInfo.values()) {
            for (Map<String, Object> eachRow : recordHighRowScore) {
                if (v.get("table_name").equals(eachRow.get("table_name"))) {
                    mergeAllRowList.add(Collections.singletonMap((String) v.get("sheet_name"),
                            Arrays.asList(((String) eachRow.get("row_name")).split("\n")[0])));
                }
            }
        }

        // 合并列数据
        List<Map<String, List<String>>> mergeAllColumnList = new ArrayList<>();
        for (Map<String, Object> v : locationSheetInfo.values()) {
            List<Map<String, Object>> columnResults = recallnodeutils.queryByColumnTextEmbedded(queryText, queryVector, v);
            if (!columnResults.isEmpty()) {
                List<Map<String, Object>> recordHighColumnScore = getHighResults(columnResults);
                for (Map<String, Object> eachColumn : recordHighColumnScore) {
                    if (v.get("table_name").equals(eachColumn.get("table_name"))) {
                        mergeAllColumnList.add(Collections.singletonMap((String) v.get("sheet_name"),
                                Arrays.asList((String) eachColumn.get("column_name"))));
                    }
                }
                // 检查工作表是否包含单位字段
                List<String> unit = recallnodeutils.getSheetNameUnit(v);
                if (unit.contains("单位")) {
                    mergeAllColumnList.add(Collections.singletonMap(
                            (String) v.get("sheet_name"), unit
                    ));
                }

            } else {
                String allColumns = recallnodeutils.getAllColumnName(v);
                mergeAllColumnList.add(Collections.singletonMap((String) v.get("sheet_name"),
                        Arrays.asList(allColumns.split("\n"))));
            }
        }

        // 行+列合并
        Map<String, Map<String, Object>> rowMerged = sameSheetContentMerged(mergeAllRowList);
        Map<String, Map<String, Object>> columnMerged = sameSheetContentMerged(mergeAllColumnList);

        // 生成完整数据
        List<Map<String, Object>> completeRowColumn = new ArrayList<>();
        for (Map<String, Object> sheetV : locationSheetInfo.values()) {
            String tableName = (String) sheetV.get("sheet_name");
            if (rowMerged.containsKey(tableName) && columnMerged.containsKey(tableName)) {
                Map<String, Object> rowColumn = new HashMap<>();
                Map<String, Object> value = new HashMap<>();
                value.put("company_name", sheetV.get("company_name"));
                value.put("sheet_name",  sheetV.get("sheet_name"));
                value.put("table_name", tableName);
                value.put("data_month", sheetV.get("data_month"));
                value.put("row_list", rowMerged.get(tableName).get("list"));
                value.put("column_list", columnMerged.get(tableName).get("list"));
                rowColumn.put(tableName, value);
                completeRowColumn.add(rowColumn);
            }
        }
        return completeRowColumn;
    }
}
