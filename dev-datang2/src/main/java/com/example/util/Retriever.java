package com.example.util;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.ContentType;
import cn.hutool.json.JSONUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONArray;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.chat.ChatLanguageModel;

import java.util.*;

public class Retriever {

    private static final String API_URL = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset-jink/api/empoworx/dataset/storeconfig/process";
    private static final String API_KEY = "sk-47c855b5e7114a02854ee2a63e472b1e";
    private static final String BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/";

    private static final ChatLanguageModel qwenClient = OpenAiChatModel.builder()
            .apiKey(API_KEY)
            .baseUrl(BASE_URL)
            .modelName("qwen-max")
            .build();

    public static String retrieveByQuestion(String userQuestion) {
        JSONObject payload = JSONUtil.createObj()
                .set("code", "")
                .set("name", "大唐集团参数表1111")
                .set("starttime", "2025-07-09 00:00:00")
                .set("endtime", "2025-07-09 23:59:59")
                .set("rowList", new JSONArray())
                .set("columnList", new JSONArray())
                .set("rowPathList", new JSONArray());

        JSONObject initResponse = postJson(API_URL, payload);
        if (initResponse == null || !initResponse.containsKey("data")) {
            return "初始数据获取失败";
        }

        JSONObject responseData = initResponse.getJSONObject("data");
        JSONArray rowData = responseData.getJSONArray("data");
        JSONArray columnList = responseData.getJSONArray("columnList");

        traverseTree(rowData);

        // JSON 示例（你可以也用外部 JSON 文件加载）
        String jsonExample = "[{\"path\":\"发电量\",\"children\":[{\"path\":\"发电量/其中：火电\",\"children\":[{\"path\":\"发电量/其中：火电/其中：煤机\",\"children\":[],\"value\":\"其中：煤机\"}],\"value\":\"其中：火电\"},{\"path\":\"发电量/水电\",\"children\":[],\"value\":\"水电\"}],\"value\":\"发电量\"},{\"path\":\"期末发电设备容量\",\"children\":[{\"path\":\"期末发电设备容量/其中：火电\",\"children\":[{\"path\":\"期末发电设备容量/其中：火电/其中：煤机\",\"children\":[],\"value\":\"其中：煤机\"}],\"value\":\"其中：火电\"},{\"path\":\"期末发电设备容量/水电\",\"children\":[],\"value\":\"水电\"}],\"value\":\"期末发电设备容量\"}]";

        String prompt = String.format(
                "你是一个专业的JSON信息检索助手，请从给定树形层次的JSON内容中检索与用户问题所有相关的信息，并且严格按照以下格式返回结果：\n" +
                        "%s\n" +
                        "重要规则：\n" +
                        "1. 只返回JSON格式的数据，不要添加任何额外的解释或说明文字。\n" +
                        "2. `value` 和 `path` 的内容必须直接来源于提供的树形层次JSON数据，不得虚构或推测。\n" +
                        "3. 根据用户的问题，仔细检索整个树形结构，确保捕捉到所有相关的节点。避免遗漏任何可能的相关节点。\n" +
                        "4. 请确保识别并使用所有与查询相关的关键词和参数，以提高检索结果的相关性。\n" +
                        "给定的JSON格式如下：\n%s" +
                        "给定的JSON格式如上：\n"+
                        "用户的问题：%s",
                jsonExample,
                JSONUtil.toJsonPrettyStr(rowData),
                userQuestion
        );

        String retrievalData = qwenClient.generate(prompt);

        JSONArray retrievedJson;
        try {
            retrievedJson = JSONUtil.parseArray(retrievalData);
        } catch (Exception e) {
            return "AI返回内容无法解析为JSON：" + e.getMessage();
        }

        List<String> rowPaths = new ArrayList<>();
        collectPaths(retrievedJson, rowPaths);
        payload.set("rowPathList", rowPaths);

        // 最终数据请求
        JSONObject finalResponse = postJson(API_URL, payload);
        if (finalResponse == null || !finalResponse.containsKey("data")) {
            return "最终数据获取失败";
        }

        JSONArray finalData = finalResponse.getJSONObject("data").getJSONArray("data");
        JSONArray tableHeaders = finalResponse.getJSONObject("data").getJSONArray("columnList");

        String htmlTable = generateHtmlTable(finalData, tableHeaders);

        // 第二次 Qwen 调用生成回答
        String secondPrompt = String.format(
                "你是一个数据问答助手，专门用于解析和回答基于表格数据的问题。你的任务是根据提供的表格数据，帮助用户找到他们需要的信息。请遵循以下指导原则来生成回答：\n" +
                "        1. **理解数据结构**：\n" +
                "           - 表格中的每一行代表一条记录或数据点。\n" +
                "           - 列标题定义了每条记录的具体属性或测量值。\n" +
                "           - 项目名称存在层次结构（例如，“项目名称”中使用“/”表示子类别），请确保在分析时考虑到这一点,并且完整输出层次结构。\n" +
                "    \n" +
                "        2. **接收并解释问题**：\n" +
                "           - 用户可能会询问关于特定列、行或者组合条件下的数据详情。\n" +
                "           - 问题可能涉及比较（如同比、环比）、求和、平均值计算等统计操作。\n" +
                "    \n" +
                "        3. **提供清晰的答案**：\n" +
                "           - 确保答案直接对应于用户的查询，并包含所有相关的细节（如数值及其单位）。\n" +
                "           - 当涉及到复杂计算或逻辑推理时，请详细说明步骤或依据。\n" +
                "    \n" +
                "        4. **格式化输出**：\n" +
                "           - 使用易于理解的语言表述结果。\n" +
                "           - 对于包含多个部分的回答，采用列表形式展示信息。\n" +
                "           - 一定要输出层次结构中的完整项目名称\n" +
                "    \n" +
                "    \n" +
                "        提供的表格数据如下：\n" +
                "        %s\n" +
                "        提供的表格数据如上\n" +
                "    \n" +
                "        用户问题：%s ",
                htmlTable, userQuestion
        );

        String answer = qwenClient.generate(secondPrompt);
        System.out.println("<--最后的答案-->\n"+answer);
        return "【生成回答】\n" + answer + "\n\n【表格HTML】\n" + htmlTable;
    }

    private static JSONObject postJson(String url, JSONObject body) {
        try (HttpResponse response = HttpRequest.post(url)
                .contentType(ContentType.JSON.toString())
                .body(body.toString())
                .execute()) {
            return JSONUtil.parseObj(response.body());
        } catch (Exception e) {
            System.err.println("请求失败：" + e.getMessage());
            return null;
        }
    }

    private static void traverseTree(JSONArray nodes) {
        for (Object obj : nodes) {
            if (!(obj instanceof JSONObject)) continue;
            JSONObject node = (JSONObject) obj;
            node.remove("jsonObject");
            node.set("path", node.getStr("path").replace(".", "/"));
            for (String key : node.keySet()) {
                Object child = node.get(key);
                if (child instanceof JSONArray) {
                    traverseTree((JSONArray) child);
                } else if (child instanceof JSONObject) {
                    traverseTree(new JSONArray().put(child));
                }
            }
        }
    }

    private static void collectPaths(JSONArray tree, List<String> paths) {
        for (Object o : tree) {
            JSONObject obj = (JSONObject) o;
            if (obj.containsKey("path")) {
                paths.add(obj.getStr("path").replace("/", "."));
            }
            if (obj.containsKey("children")) {
                collectPaths(obj.getJSONArray("children"), paths);
            }
        }
    }

    private static String generateHtmlTable(JSONArray data, JSONArray headers) {
        StringBuilder sb = new StringBuilder("<table border='1'><tr>");
        for (Object h : headers) sb.append("<th>").append(h).append("</th>");
        sb.append("</tr>");
        for (Object obj : data) {
            JSONObject item = (JSONObject) obj;
            JSONObject jsonObj = item.getJSONObject("jsonObject");
            sb.append("<tr>");
            for (Object h : headers) {
                String key = (String) h;
                String value = jsonObj.getStr(key, "");
                if ("项    目".equals(key)) {
                    value = jsonObj.getStr("dspath", "").replace(".", "/");
                }
                sb.append("<td>").append(value).append("</td>");
            }
            sb.append("</tr>");
        }
        sb.append("</table>");
        return sb.toString();
    }
}
