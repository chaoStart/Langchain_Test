package com.example.controller;

import com.example.dto.MessageListRequest;
import com.example.dto.MessagePayload;
import com.example.service.QuestionRewriteService;
import dev.langchain4j.data.message.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api")
public class QuestionRewriteController {
    private final QuestionRewriteService rewriteServer;

    @Autowired
    public QuestionRewriteController(QuestionRewriteService rewriteServer) {
        this.rewriteServer = rewriteServer;
    }
    @PostMapping("/rewrite")
    public String rewriteQuestion(@RequestBody MessageListRequest request) {
        List<MessagePayload> payloads = request.getMessages();
        List<ChatMessage> messages = new ArrayList<>();

        for (MessagePayload payload : payloads) {
            String role = payload.getRole();
            String content = payload.getContent();

            if ("user".equalsIgnoreCase(role)) {
                messages.add(UserMessage.from(content));
            } else if ("assistant".equalsIgnoreCase(role)) {
                messages.add(AiMessage.from(content));
            } else if ("system".equalsIgnoreCase(role)) {
                messages.add(SystemMessage.from(content));
            } else {
                // 忽略或抛异常
                throw new IllegalArgumentException("不支持的角色类型: " + role);
            }
        }

        return rewriteServer.rewriteQuestion(messages);
    }
}