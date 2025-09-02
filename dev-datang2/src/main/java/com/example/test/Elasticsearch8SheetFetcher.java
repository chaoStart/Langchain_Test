package com.example.test;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
//import co.elastic.clients.elasticsearch.indices.ExistsResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.gson.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class Elasticsearch8SheetFetcher {

    private static final String URL = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset-jink/api/empoworx/dataset/storeconfig/process";
    private static final String INDEX_NAME = "storage_dataset_big_chunk";

    private static final RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
    private static final ElasticsearchClient esClient = new ElasticsearchClient(
            new RestClientTransport(restClient, new JacksonJsonpMapper())
    );

    public static class SheetInfo {
        public String company_name;
        public String data_month;
        public String sheet_name;
        public String row_name;
        public String column_name;
    }

    public static void main(String[] args) throws Exception {
        JsonObject payload = buildPayload();
        //调用接口，获取数据集的sheet和对应的整行整列名称
        JsonObject responseJson = fetchData(payload);
        if (responseJson == null || !responseJson.has("data")) return;
        //解析数据,获取数据集中全部的 sheet
        JsonObject allSheets = responseJson.getAsJsonObject("data");
        List<SheetInfo> allSheetList = new ArrayList<>();

        for (Map.Entry<String, JsonElement> entry : allSheets.entrySet()) {
            //遍历获取其中的公司名称，数据月份，sheet名称，行名称，列名称
            JsonObject oneSheet = entry.getValue().getAsJsonObject();
            SheetInfo info = new SheetInfo();
            info.company_name = oneSheet.get("\u7f16\u5236\u5355\u4f4d").getAsString();
            info.data_month = oneSheet.get("\u6570\u636e\u7f16\u5236\u65f6\u95f4").getAsString();
            info.sheet_name = oneSheet.get("sheet\u540d\u79f0").getAsString();
            //获取列名称
            JsonArray columnArray = oneSheet.getAsJsonArray("columnList");
            List<String> colList = new ArrayList<>();
            for (JsonElement col : columnArray) {
                colList.add(col.getAsString());
            }
            info.column_name = String.join(" ", colList);
            //获取行名称(recurseChildren递归拼接每一行row的名称为一个大的行名)
            JsonArray rowArray = oneSheet.getAsJsonArray("data");
            List<String> rowNameList = new ArrayList<>();
            for (JsonElement rowItem : rowArray) {
                List<String> nameList = new ArrayList<>();
                recurseChildren(rowItem.getAsJsonObject(), nameList);
                rowNameList.add(String.join("\n", nameList));
            }
            info.row_name = String.join("\n", rowNameList);
            allSheetList.add(info);//添加到列表中,待写入Elasticsearch
        }

        System.out.println("\ud83e\udde0 成功获取数据集的sheet和对应的整行整列名称");
        storeToElasticsearch(allSheetList);
    }

    private static JsonObject buildPayload() {
        JsonObject payload = new JsonObject();
        payload.addProperty("code", "");
        payload.addProperty("name", "");
        payload.addProperty("companyName", "大唐集团吕四分公司");
        payload.add("datasetIds", new JsonArray());
        payload.addProperty("queryType", "1");
        payload.addProperty("starttime", "2025-07-16 00:00:00");
        payload.addProperty("endtime", "2025-07-16 23:59:59");
        payload.add("rowList", new JsonArray());
        payload.add("columnList", new JsonArray());
        payload.add("rowPathList", new JsonArray());
        payload.add("columnParam", new JsonObject());
        return payload;
    }

    private static JsonObject fetchData(JsonObject payload) {
        try (HttpResponse response = HttpRequest.post(URL)
                .header("Content-Type", "application/json")
                .body(payload.toString())
                .execute()) {
            if (response.getStatus() != 200) {
                System.err.println("HTTP 错误: " + response.getStatus());
                return null;
            }
            return JsonParser.parseString(response.body()).getAsJsonObject();
        } catch (Exception e) {
            System.err.println("请求失败: " + e.getMessage());
            return null;
        }
    }

    private static void recurseChildren(JsonObject item, List<String> nameList) {
        nameList.add(item.get("value").getAsString());
        if (item.has("children")) {
            JsonArray children = item.getAsJsonArray("children");
            for (JsonElement child : children) {
                recurseChildren(child.getAsJsonObject(), nameList);
            }
        }
    }

    private static void storeToElasticsearch(List<SheetInfo> sheetList) throws IOException {
        boolean exists = esClient.indices().exists(e -> e.index(INDEX_NAME)).value();
        if (!exists) {
            createIndex();
        } else {
            System.out.println("⚠️ 索引已存在");
        }

        BulkRequest.Builder br = new BulkRequest.Builder();
        for (SheetInfo sheet : sheetList) {
            br.operations(op -> op.index(idx -> idx.index(INDEX_NAME).document(sheet)));
        }

        BulkResponse result = esClient.bulk(br.build());
        System.out.println("✅ 成功写入 " + result.items().size() + " 条文档");
    }

    private static void createIndex() throws IOException {
        String settingsJson = "{\n" +
                "  \"analysis\": {\n" +
                "    \"analyzer\": {\n" +
                "      \"ik_max_word_analyzer\": {\n" +
                "        \"type\": \"custom\",\n" +
                "        \"tokenizer\": \"ik_max_word\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String mappingsJson = "{\n" +
                "  \"properties\": {\n" +
                "    \"company_name\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\", \"search_analyzer\": \"ik_smart\"},\n" +
                "    \"data_month\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\", \"search_analyzer\": \"ik_smart\"},\n" +
                "    \"sheet_name\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\", \"search_analyzer\": \"ik_smart\"},\n" +
                "    \"row_name\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\", \"search_analyzer\": \"ik_smart\"},\n" +
                "    \"column_name\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\", \"search_analyzer\": \"ik_smart\"}\n" +
                "  }\n" +
                "}";

        CreateIndexRequest request = new CreateIndexRequest.Builder()
                .index(INDEX_NAME)
                .settings(s -> s.withJson(new StringReader(settingsJson)))
                .mappings(TypeMapping.of(m -> m.withJson(new StringReader(mappingsJson))))
                .build();

        esClient.indices().create(request);
        System.out.println("✅ 已创建索引并启用 IK 分词器");
    }
}
