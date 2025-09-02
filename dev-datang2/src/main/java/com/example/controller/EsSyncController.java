package com.example.controller;

import com.example.service.SheetSyncService;
import com.example.dto.EsSyncRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/*这里是一个接口，用于同步财务数据到Elasticsearch。*/

@RestController
@RequestMapping("/api/sheet")
public class EsSyncController {
    @Autowired
    private  SheetSyncService sheetSyncService;

    @PostMapping("/sync")
    public String syncSheets(@RequestBody EsSyncRequest request) {
        try {
            int count = sheetSyncService.fetchAndStore(request);
            return String.format("✅ 同步成功 !!! ⏱ 成功写入 + %s 条文档 !!!", count);
        } catch (Exception e) {
            return "❌ 同步失败: " + e.getMessage();
        }
    }
}
