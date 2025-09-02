// com.example.service.SheetSyncService.java
package com.example.service;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import com.example.dto.EsSyncRequest;
import com.example.dto.SheetInfo;
import com.google.gson.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

@Service
public class SheetSyncService {

    @Value("${elasticsearch.index}")
    private String indexName;

    @Value("${dataset_URL}")
    private String URL ;

    @Autowired
    private ElasticsearchClient esClient;

    public int fetchAndStore(EsSyncRequest request) throws IOException {
        JsonObject payload = buildPayload(request);
        JsonObject responseJson = fetchData(payload);
        if (responseJson == null || !responseJson.has("data")) return 0;

        List<SheetInfo> allSheetList = parseSheets(responseJson.getAsJsonObject("data"));

        return storeToElasticsearch(allSheetList);
    }

    public JsonObject buildPayload(EsSyncRequest request) {
        JsonObject payload = new JsonObject();
        payload.addProperty("companyName", request.getCompanyName());
        payload.addProperty("queryType", request.getQueryType());
        payload.addProperty("starttime", request.getStarttime());
        payload.addProperty("endtime", request.getEndtime());

        JsonArray datasetArray = new JsonArray();
        for (String id : request.getDatasetIds()) {
            datasetArray.add(id);
        }
        payload.add("datasetIds", datasetArray);

        JsonArray rowArray = new JsonArray();
        for (String row : request.getRowList()) {
            rowArray.add(row);
        }
        payload.add("rowList", rowArray);

        JsonArray columnArray = new JsonArray();
        for (String col : request.getColumnList()) {
            columnArray.add(col);
        }
        payload.add("columnList", columnArray);

        JsonArray rowPathArray = new JsonArray();
        for (String path : request.getRowPathList()) {
            rowPathArray.add(path);
        }
        payload.add("rowPathList", rowPathArray);

        JsonObject columnParamObj = new Gson().toJsonTree(request.getColumnParam()).getAsJsonObject();
        payload.add("columnParam", columnParamObj);

        return payload;
    }


    private JsonObject fetchData(JsonObject payload) {
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

    private List<SheetInfo> parseSheets(JsonObject allSheets) {
        List<SheetInfo> list = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : allSheets.entrySet()) {
            JsonObject sheet = entry.getValue().getAsJsonObject();
            SheetInfo info = new SheetInfo();
            info.company_name = sheet.get("\u7f16\u5236\u5355\u4f4d").getAsString();
            info.data_month = sheet.get("\u6570\u636e\u7f16\u5236\u65f6\u95f4").getAsString();
            info.sheet_name = sheet.get("sheet\u540d\u79f0").getAsString();

            JsonArray columnArray = sheet.getAsJsonArray("columnList");
            List<String> columns = new ArrayList<>();
            for (JsonElement col : columnArray) {
                columns.add(col.getAsString());
            }
            info.column_name = String.join(" ", columns);

            JsonArray rowArray = sheet.getAsJsonArray("data");
            List<String> rowNames = new ArrayList<>();
            for (JsonElement row : rowArray) {
                List<String> rowVals = new ArrayList<>();
                recurseChildren(row.getAsJsonObject(), rowVals);
                rowNames.add(String.join("\n", rowVals));
            }
            info.row_name = String.join("\n", rowNames);

            list.add(info);
        }
        return list;
    }

    private void recurseChildren(JsonObject item, List<String> nameList) {
        nameList.add(item.get("value").getAsString());
        if (item.has("children")) {
            for (JsonElement child : item.getAsJsonArray("children")) {
                recurseChildren(child.getAsJsonObject(), nameList);
            }
        }
    }

    private int storeToElasticsearch(List<SheetInfo> sheetList) throws IOException {
        if (!esClient.indices().exists(e -> e.index(indexName)).value()) {
            createIndex();
        }

        BulkRequest.Builder br = new BulkRequest.Builder();
        for (SheetInfo sheet : sheetList) {
            br.operations(op -> op.index(idx -> idx.index(indexName).document(sheet)));
        }

        BulkResponse result = esClient.bulk(br.build());
        System.out.println("✅ 成功写入 " + result.items().size() + " 条文档");
        return result.items().size();
    }

    private void createIndex() throws IOException {
        String settingsJson = "{ \"analysis\": { \"analyzer\": { \"ik_max_word_analyzer\": { \"type\": \"custom\", \"tokenizer\": \"ik_max_word\" } } } }";
        String mappingsJson = "{ \"properties\": { " +
                "\"company_name\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\"}, " +
                "\"data_month\": {\"type\": \"text\"}," +
                " \"sheet_name\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\"}, " +
                "\"row_name\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\"}, " +
                "\"column_name\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\"} } }";

        CreateIndexRequest request = new CreateIndexRequest.Builder()
                .index(indexName)
                .settings(s -> s.withJson(new StringReader(settingsJson)))
                .mappings(TypeMapping.of(m -> m.withJson(new StringReader(mappingsJson))))
                .build();

        esClient.indices().create(request);
        System.out.println("✅ 已创建索引并启用 IK 分词器");
    }
}
