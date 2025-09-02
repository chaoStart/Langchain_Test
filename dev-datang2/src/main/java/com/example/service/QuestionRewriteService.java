package com.example.service;

import cn.hutool.core.util.ReUtil;
import com.example.util.PromptBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.data.message.ChatMessage;

import java.util.List;

import org.springframework.stereotype.Service;

@Service
public class QuestionRewriteService {

    private static final String API_KEY = "sk-47c855b5e7114a02854ee2a63e472b1e"; // 请替换为你的API密钥
    private static final String BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/";

    private static final Gson gson = new Gson();
    public String rewriteQuestion(List<ChatMessage> messages) {
        // 1. 构建历史消息

        // 2. 构建提示词（csdn_rewrite_prompt 替代）
        String csdnPrompt = PromptBuilder.buildCsdnRewritePrompt(messages);

        // 3. 调用千问 API（qwen-max）
        String result = callQwen(csdnPrompt);

        System.out.println("CSDN的提示模板: \n" + result);
        System.out.println("---------------------------------------------------------------");
        // 4. 正则提取 JSON 中的 new_question 字段
        String jsonStr = ReUtil.get("\\{[^{}]*\"new_question\"[^{}]*\\}", result, 0);
        String newQuestion = null; // 在外部定义

        if (jsonStr != null) {
            JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class);
            newQuestion = jsonObject.get("new_question").getAsString();
            System.out.println("抽取结果： " + newQuestion);
        } else {
            System.out.println("未找到符合条件的 JSON 结构,可能是大模型理解用户意图出错~");
            newQuestion = "请重新提问";
        }
        return newQuestion ;
    }


    private static String callQwen(String prompt) {
        OpenAiChatModel model = OpenAiChatModel.builder()
                .apiKey(API_KEY)
                .baseUrl(BASE_URL)
                .modelName("qwen-max")
                .build();

        return model.generate(prompt);
    }
}
