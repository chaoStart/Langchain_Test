package com.example.entity;

// 内部类：指标数据结构
public class Indicator {
    int index;
    String title;
    String content;

     public Indicator(int index, String title, String content) {
        this.index = index;
        this.title = title;
        this.content = content;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getIndex() {
        return index;
    }

    public String getTitle() {
        return title;
    }

    public String getContent() {
        return content;
    }
}
