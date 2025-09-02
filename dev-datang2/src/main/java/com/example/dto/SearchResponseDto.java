package com.example.dto;

public class SearchResponseDto {
    private Double score;
//    private Integer index;
    private String id;
    private String title;
    private String content;

    public SearchResponseDto(Double score,String id,String title, String content) {
        this.score = score;
//        this.index = index;
        this.id = id;
        this.title = title;
        this.content = content;
    }

    public Double getScore() {
        return score;
    }

    public String getId() {return id;}

    public String getTitle() {
        return title;
    }

    public String getContent() {
        return content;
    }
}

