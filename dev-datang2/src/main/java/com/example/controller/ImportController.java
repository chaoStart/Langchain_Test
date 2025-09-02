package com.example.controller;

import com.example.util.EsDataImporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import javax.annotation.Resource;

@RestController
@RequestMapping("/api")
public class ImportController {

    private final EsDataImporter esDataImporter;
    public ImportController(EsDataImporter esDataImporter) {
        this.esDataImporter = esDataImporter;
    }

    @PostMapping("/import-excel")
    public ResponseEntity<String> importExcelToES() {
        try {
            esDataImporter.importExcelToES();
            return ResponseEntity.ok("✅ Excel 数据已成功导入 Elasticsearch！");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("❌ 导入失败：" + e.getMessage());
        }
    }
}
