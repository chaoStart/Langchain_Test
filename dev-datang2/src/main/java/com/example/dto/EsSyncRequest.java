package com.example.dto;

import java.util.List;
import java.util.Map;

public class EsSyncRequest {
    private String companyName;
    private String queryType;
    private String starttime;
    private String endtime;
    private List<String> datasetIds;
    private List<String> rowList;
    private List<String> columnList;
    private List<String> rowPathList;
    private Map<String, Object> columnParam;

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getStarttime() {
        return starttime;
    }

    public void setStarttime(String starttime) {
        this.starttime = starttime;
    }

    public String getEndtime() {
        return endtime;
    }

    public void setEndtime(String endtime) {
        this.endtime = endtime;
    }

    public List<String> getDatasetIds() {
        return datasetIds;
    }

    public void setDatasetIds(List<String> datasetIds) {
        this.datasetIds = datasetIds;
    }

    public List<String> getRowList() {
        return rowList;
    }

    public void setRowList(List<String> rowList) {
        this.rowList = rowList;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<String> columnList) {
        this.columnList = columnList;
    }

    public List<String> getRowPathList() {
        return rowPathList;
    }

    public void setRowPathList(List<String> rowPathList) {
        this.rowPathList = rowPathList;
    }

    public Map<String, Object> getColumnParam() {
        return columnParam;
    }

    public void setColumnParam(Map<String, Object> columnParam) {
        this.columnParam = columnParam;
    }
}
