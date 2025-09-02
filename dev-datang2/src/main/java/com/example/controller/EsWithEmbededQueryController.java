package com.example.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.example.dto.SearchResponseDto;
import com.example.service.EsSearchWithEmbeddedService;
import com.example.dto.QueryRequest;
import com.example.util.PromptBuilder;
import com.google.gson.Gson;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/query")
public class EsWithEmbededQueryController {
    // 由 Spring 注入
    @Autowired
    private ElasticsearchClient esClient;
    // 由 Spring 注入
    @Autowired
    private OpenAiChatModel model;

    @Autowired
    private EsSearchWithEmbeddedService searchService;

    @Value("${external.indicator_es_index}")
    private String INDEX_NAME;

    @PostMapping("/index")
    public List<String> queryMostRelevantIndex(@RequestBody QueryRequest request) throws IOException {
        // 0. 获取 user_question
        String userQuery = request.getQuery() ;
        // ✅ Step 1: 使用 SearchService 执行嵌入式搜索
//        List<SearchResponseDto> results = searchService.searchWithEmbedding(userQuery);
        List<SearchResponseDto> results = searchService.searchHybridMerged(userQuery);
        Map<String, Map<String, Object>> relevantIndex = new HashMap<>();

        for (SearchResponseDto dto : results) {
            Map<String, Object> doc = new HashMap<>();
//            String idx = String.valueOf(dto.getId());
            doc.put("Id", dto.getId());
            doc.put("title", dto.getTitle());
            doc.put("content", dto.getContent());
            doc.put("score", dto.getScore());

            relevantIndex.put(dto.getId(),doc);

            System.out.println("- Id: " + dto.getId());
            System.out.println("  Title: " + dto.getTitle());
            System.out.println("  Content: " + dto.getContent());
            System.out.println("  Score: " + dto.getScore());
        }

        System.out.println("\nES数据库中检索到的所有与用户相关的index指标名称:");
        System.out.println(new Gson().toJson(relevantIndex));
        // 2. 构造 Prompt
        String prompt = PromptBuilder.buildChooseIndexPrompt(relevantIndex, userQuery);

        // 3. 调用大模型
        String key = model.generate(prompt); // 使用 Spring 注入的 LLM 实例
        System.out.println("打印最后大模型优化返回索引值:"+ key);

        // 4. 返回结果（你也可以改为返回 JSON）
        String[] index_key = new ObjectMapper().readValue(key, String[].class);
        System.out.println("key 原始内容：" + key);

        // 5. 根据 index_key 把对应 content 取出来
        List<String> matchedContents = new ArrayList<>();
        for (String idx : index_key) {
            for (SearchResponseDto dto : results) {
                if (dto.getId().equals(idx)) {          // 找到同索引的记录
                    matchedContents.add(dto.getContent());
                    break;                            // 一条索引只取一次就够了
                }
            }
        }
        // 6. 打印或返回
        System.out.println("=== 大模型选中的 content ===");
        matchedContents.forEach(System.out::println);
        return matchedContents;
    }
}

