package com.example.dto;

public class IndicatorRequest {
    private String companyName;
    private String startTime;
    private String endTime;
    // 你可以继续添加需要的字段

    // 构造器、getter、setter
    public IndicatorRequest() {}

    public String getCompanyName() {
        return companyName;
    }
    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getStartTime() {
        return startTime;
    }
    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }
    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }
}
