package com.example.util;

import com.google.gson.Gson;
import java.util.Map;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class PromptBuilder {

    public static String buildCsdnRewritePrompt(List<ChatMessage> messages) {
        List<String> convList = new ArrayList<>();

        for (ChatMessage message : messages) {
            String role = null;
            String content = message.text();
            if (message instanceof UserMessage) {
                role = "USER";
            } else if (message instanceof AiMessage) {
                role = "ASSISTANT";
            }

            if (role != null) {
                convList.add(role + ": " + content);
            }
        }

        // 构造历史对话和最新问题
        List<String> history = convList.subList(0, convList.size() - 1);
        String latestQuestion = convList.get(convList.size() - 1).replace("USER: ", "").trim();
        String historyConv = String.join("\n", convList);

        // 日期
        String today = LocalDate.now().toString();
        String yesterday = LocalDate.now().minusDays(1).toString();
        String tomorrow = LocalDate.now().plusDays(1).toString();

        return String.format(
                "您将获得以下两个要素：\n" +
                        "\t1. **用户的新问题**\n" +
                        "\t2. **历史聊天记录**\n" +
                        "\n" +
                        "您的任务是**根据获得的两个要素去创建一个新的、更好的问题**，以帮助语义搜索系统（如基于向量的检索系统）更准确地找到相关信息。\n" +
                        "\n" +
                        "### 🔍 按照以下清晰步骤操作：\n" +
                        "\n" +
                        "**步骤 1： 理解原始问题。**\n" +
                        "确定问题所询问的内容——重点关注其指代的指标名称、日期时间或公司名称。\n" +
                        "\n" +
                        "**步骤2：从历史聊天记录提取关键细节。**\n" +
                        "仔细阅读历史聊天记录，并找出最重要的信息——尤其是指标名称、时间日期、公司名称。\n" +
                        "👉 **您必须将这些关键信息包含在新问题中。*** *\n" +
                        "\n" +
                        "**步骤3：创建一个自然的后续问题。**\n" +
                        "现在，请思考一个新问题，该问题需：\n" +
                        "* 聚焦于历史聊天记录中识别出的公司名称、时间日期和指标名称。\n" +
                        "* 将对话引导至原问题所关注的内容（但以更清晰或更直接的方式）。\n" +
                        "\n" +
                        "**步骤4：清晰完整地撰写新问题。**\n" +
                        "你的最终问题必须：\n" +
                        "包含历史聊天记录中提到的公司名称、时间日期和指标名称。\n" +
                        "与原话题直接相关。\n" +
                        "使搜索系统更容易检索到正确答案。\n" +
                        "\n" +
                        "### 🚫 要求和限制：\n" +
                        "* 不要省略历史聊天记录中提到的公司名称、时间日期和指标名称。\n" +
                        "* 不要完全重复原问题。\n" +
                        "* 如果历史聊天记录没有公司名称和指标名称信息，则不要捏造这2个关键信息。\n" +
                        "* 如果用户的文本内容涉及到相对日期，你需要将其转换为绝对日期，基于当前日期 %s。例如：'昨天'会被转换为 %s。\n" +
                        "* 如果用户的文本内容没有涉及到相对日期，那么默认日期为当前日期 %s"+
                        "* 所有实体公司之间不存在上下级所属关系,如果出现了多个实体公司名称，应该处理为多个完整的新问题"+
                        "\n" +
                        "### ✅ 示例（仅供参考 - 切勿复制）：\n" +
                        "# 例子 1\n" +
                        "## 对话\n" +
                        "USER: 唐纳德-特朗普的父亲是谁?\n" +
                        "ASSISTANT:  弗雷德-特朗普.\n" +
                        "USER: 母亲是谁?\n" +
                        "###############\n" +
                        "new_question: {{唐纳德-特朗普的母亲叫什么名字？}}\n" +
                        "\n" +
                        "------------\n" +
                        "# 例子 2\n" +
                        "## 对话\n" +
                        "USER: 你好，这个苹果是多少钱？\n" +
                        "ASSISTANT:  一个苹果的价格是3元？\n" +
                        "USER: 一个橙子的价格是多少钱？\n" +
                        "ASSISTANT:  一个橙子的价格是2.5元？\n" +
                        "User: 香蕉呢?\n" +
                        "###############\n" +
                        "new_question: {{那香蕉的价格是多少钱一斤?}}\n" +
                        "\n" +
                        "------------\n" +
                        "# 例子 3\n" +
                        "## 对话\n" +
                        "USER: 今天伦敦的天气是怎么样的?\n" +
                        "ASSISTANT: 阴天.\n" +
                        "USER: 明天悉尼呢?\n" +
                        "###############\n" +
                        "new_question: {{悉尼在明天 %s 的天气是怎么样的?}}\n" +
                        "\n" +
                        "### 下面是你获得的两个要素：\n" +
                        "- 用户的新问题：%s\n" +
                        "- 历史聊天记录：%s\n" +
                        "\n" +
                        "### 🎯 输出格式：\n" +
                        "1.\n" +
                        "一个多步骤、逻辑连贯的解释，展示你的推理过程。\n" +
                        "2.\n" +
                        "末尾包含最终推断问题的JSON块。\n" +
                        "\n" +
                        "{\n" +
                        "\"new_question\": \"您清晰、具体且包含实体的提问内容在此处。\"\n" +
                        "}",
                today, yesterday,today, tomorrow, latestQuestion, historyConv);
    }

    public static String buildChooseIndexPrompt(Map<String, Map<String, Object>> retrieverFromEs, String question) {
        Gson gson = new Gson();
        String jsonContent = gson.toJson(retrieverFromEs);

        return String.format(
                    "### 角色\n"+
                       " 你是一名检索专家，能够从提供的结构化数据中选择与用户问题最相关的索引ID。\n"+

                    "### 指令\n"+
                        "1.首先，准确读取结构化的字典数据，明确字典数据中每一条索引值ID对应的文字内容；\n"+
                        "2.然后，分析字典数据中每一条数据中的content与用户的问题question之间的相关性。\n"+
                        "3.最后，从结构化字典数据中选择content与用户问题最相关的数据，并返回该字典数据对应的索引值ID。\n"+

                    "### 格式要求和限制\n"+
                        "1、只需要返回对应的索引值ID；\n"+
                        "2、返回的形式是一个包含索引值ID的数组，\n" +
                        "3、如果存在多个content与用户question相关，则返回的数组可以包含多个索引值ID\n"+

                   " ### 请根据下面提供的结构化数据和用户问题，返回包含索引值的数组\n"+
                        "-content:%s\n"+
                        "-question:%s\n",jsonContent,question
        );
    }
}
