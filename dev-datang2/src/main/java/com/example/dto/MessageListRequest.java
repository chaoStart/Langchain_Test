package com.example.dto;

import java.util.List;

public class MessageListRequest {
    private List<MessagePayload> messages;

    public List<MessagePayload> getMessages() {
        return messages;
    }

    public void setMessages(List<MessagePayload> messages) {
        this.messages = messages;
    }
}
