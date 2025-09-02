package com.example.controller;

import com.example.dto.IndicatorRequest;
import com.example.service.DatangIndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/api/import")
public class DataImportController {

    private final DatangIndexService datangindexservice;

    @Autowired
    public DataImportController(DatangIndexService datangindexservice) {
        this.datangindexservice = datangindexservice;
    }

    /**
     * 触发数据导入
     * @return 操作结果提示
     */
    @PostMapping("/datang")
    public String importDatangData(@RequestBody IndicatorRequest req) {
        try {
            datangindexservice.index(req);
            return "✅ 大唐数据导入成功";
        } catch (IOException e) {
            e.printStackTrace();
            return "❌ 导入失败: " + e.getMessage();
        }
    }
}

