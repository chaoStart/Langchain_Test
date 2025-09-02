package com.example.controller;

import com.example.util.StorageFinancialData2Es;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class FinancialDataProcessorCopy {

    private static final String API_URL = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/storeconfig/process";
    private static final String EMBEDDING_URL = "http://10.3.24.46:9997/v1/embeddings";
    private static final String API_KEY = "123";

    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("code", "");
        payload.put("name", "");
        payload.put("companyName", "");
        payload.put("datasetIds",  Arrays.asList("668430957309526066"));
        payload.put("queryType", "1");
        payload.put("starttime", "2025-08-01");
        payload.put("endtime", "2025-08-01");
        payload.put("rowList", new ArrayList<>());
        payload.put("columnList", new ArrayList<>());
        payload.put("rowPathList", new ArrayList<>());
        payload.put("columnParam", new HashMap<>());

        JsonObject data = fetchData(API_URL, gson.toJson(payload));
        if (data == null) {
            System.out.println("❌ 请求失败，退出");
            return;
        }

        JsonObject allSheetData = data.getAsJsonObject("data");
        List<Map<String, Object>> allSheetList = new ArrayList<>();

        // 处理行数据
        for (Map.Entry<String, JsonElement> entry : allSheetData.entrySet()) {
            JsonObject oneSheetData = entry.getValue().getAsJsonObject();
            String companyName = oneSheetData.get("编制单位").getAsString();
            String dataMonth = oneSheetData.get("数据编制时间").getAsString();
            String sheetName = oneSheetData.get("sheet名称").getAsString();
            String tableName = oneSheetData.get("标题名称").getAsString();

            JsonArray dataArr = oneSheetData.getAsJsonArray("data");
            if (dataArr.size() == 0) continue;
            JsonArray rowListData = dataArr.get(0).getAsJsonObject().getAsJsonArray("data");
            if (rowListData.size() == 0) continue;

            if (rowListData.size() == 1) {
                JsonObject item = rowListData.get(0).getAsJsonObject();
                // 存储所有子节点的文本内容（合并成为一个大字段）
                List<String> allNodeLeaf = new ArrayList<>();
                List<String> nodeLeaf = new ArrayList<>();
                recursionRowChildrenAll(item, nodeLeaf);
                allNodeLeaf.addAll(nodeLeaf); //合并所有子项的内容
                //拼接为一个大字符串
                String bigRowName = String.join("\n", allNodeLeaf);
                //生成对应的嵌入项目模型
                List<Double> embedding = getEmbedding(bigRowName);
                // 构建一个字典，只添加一次
                Map<String, Object> sheetDict = new HashMap<>();
                sheetDict.put("company_name", companyName);
                sheetDict.put("data_month", dataMonth);
                sheetDict.put("sheet_name", sheetName);
                sheetDict.put("table_name", tableName);
                sheetDict.put("sheet_type", "row_data");
                sheetDict.put("row_name", bigRowName);
                sheetDict.put("embedding", embedding);

                allSheetList.add(sheetDict);

            } else {
                for (int i = 0; i < rowListData.size(); i++) {
                    JsonObject rowNode = rowListData.get(i).getAsJsonObject();
                    List<String> nodeLeaf = new ArrayList<>();
                    recursionRowChildrenAll(rowNode, nodeLeaf);
                    String nodeContent = String.join("\n", nodeLeaf);
                    String nodePath = rowNode.get("path").getAsString();

                    Map<String, Object> sheetDict = new HashMap<>();
                    sheetDict.put("company_name", companyName);
                    sheetDict.put("data_month", dataMonth);
                    sheetDict.put("sheet_name", sheetName);
                    sheetDict.put("table_name", tableName);
                    sheetDict.put("sheet_type", "row_data");
                    sheetDict.put("row_name", nodeContent);
                    sheetDict.put("embedding", getEmbedding(nodeContent));

                    allSheetList.add(sheetDict);
                }
            }
        }

        // 处理列数据
        for (Map.Entry<String, JsonElement> entry : allSheetData.entrySet()) {
            JsonObject oneSheetData = entry.getValue().getAsJsonObject();
            String companyName = oneSheetData.get("编制单位").getAsString();
            String dataMonth = oneSheetData.get("数据编制时间").getAsString();
            String sheetName = oneSheetData.get("sheet名称").getAsString();
            String tableName = oneSheetData.get("标题名称").getAsString();

            JsonArray columnArr = oneSheetData.getAsJsonArray("columnList");
            if (columnArr == null || columnArr.size() == 0) {
                System.out.println("空列名称，跳过");
                continue;
            }

            List<String> currentColumnList = new ArrayList<>();
            Map<String, String> currentColumnNode = new HashMap<>();

            for (JsonElement elem : columnArr) {
                String cValue = elem.getAsString();
                if (!cValue.contains(".")) {
                    currentColumnList.add(cValue);
                } else {
                    String[] parts = cValue.split("\\.", 2);
                    String key = parts[0];
                    String childValue = parts[1];
                    currentColumnNode.put(key, currentColumnNode.getOrDefault(key, "") + "\n" + childValue);
                }
            }

            for (Map.Entry<String, String> colEntry : currentColumnNode.entrySet()) {
                currentColumnList.add(colEntry.getKey() + "\n" + colEntry.getValue());
            }

            for (String columnData : currentColumnList) {
                Map<String, Object> sheetDict = new HashMap<>();
                sheetDict.put("company_name", companyName);
                sheetDict.put("data_month", dataMonth);
                sheetDict.put("sheet_name", sheetName);
                sheetDict.put("table_name", tableName);
                sheetDict.put("sheet_type", "column_data");
                sheetDict.put("column_name", columnData);
                sheetDict.put("embedding", getEmbedding(columnData));

                allSheetList.add(sheetDict);
            }
        }

        System.out.println("🧠成功获取数据集的sheet和对应的整行整列名称");
        StorageFinancialData2Es.save(allSheetList);
    }

    // 递归处理 children 节点
    private static void recursionRowChildrenAll(JsonObject item, List<String> nameList) {
        JsonArray children = item.getAsJsonArray("children");
        if (children != null && children.size() > 0) {
            nameList.add(item.get("value").getAsString());
            for (int i = 0; i < children.size(); i++) {
                recursionRowChildrenAll(children.get(i).getAsJsonObject(), nameList);
            }
        } else {
            nameList.add(item.get("value").getAsString());
        }
    }

    // 获取数据
    private static JsonObject fetchData(String url, String jsonPayload) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(url);
            post.setHeader("Content-Type", "application/json");
            post.setEntity(new StringEntity(jsonPayload, StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(post)) {
                String result = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                return gson.fromJson(result, JsonObject.class);
            }
        } catch (IOException e) {
            System.out.println("HTTP 请求失败: " + e.getMessage());
            return null;
        }
    }

    // 调用 OpenAI Embedding
    private static List<Double> getEmbedding(String text) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(EMBEDDING_URL);
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Authorization", "Bearer " + API_KEY);

            JsonObject payload = new JsonObject();
            payload.addProperty("model", "bge-large-zh-v1.5");
            JsonArray arr = new JsonArray();
            arr.add(text);
            payload.add("input", arr);

            post.setEntity(new StringEntity(payload.toString(), StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(post)) {
                String result = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                JsonObject obj = gson.fromJson(result, JsonObject.class);
                JsonArray embeddingArr = obj.getAsJsonArray("data")
                        .get(0).getAsJsonObject()
                        .getAsJsonArray("embedding");

                List<Double> embedding = new ArrayList<>();
                for (JsonElement el : embeddingArr) {
                    embedding.add(el.getAsDouble());
                }
                return embedding;
            }
        } catch (IOException e) {
            System.out.println("Embedding 请求失败: " + e.getMessage());
            return Collections.emptyList();
        }
    }
}

