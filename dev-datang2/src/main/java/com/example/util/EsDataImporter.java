package com.example.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.*;
import java.util.*;

@Component
public class EsDataImporter {

    @Resource
    private ElasticsearchClient esClient;

    @Value("${indicator.INDEX_NAME}")
    private  String INDEX_NAME ;

    @Value("${indicator.EXCEL_PATH}")
    private String EXCEL_PATH ;

    @Value("${indicator.SHEET_NAME}")
    private String SHEET_NAME;

    public void importExcelToES() throws Exception {
        List<EsDoc> docs = readExcelAndDeduplicate(EXCEL_PATH, SHEET_NAME);

        // 创建索引及映射
        createIndexIfNotExists();

        // 批量写入 ES
        List<BulkOperation> bulkOperations = new ArrayList<>();
        for (EsDoc doc : docs) {
            bulkOperations.add(BulkOperation.of(op -> op
                    .index(idx -> idx
                            .index(INDEX_NAME)
                            .document(doc)
                    )
            ));
        }

        esClient.bulk(BulkRequest.of(b -> b.index(INDEX_NAME).operations(bulkOperations)));
        System.out.println("✅ 成功写入 " + bulkOperations.size() + " 条文档到 Elasticsearch");
    }

    // 读取 Excel 文件并去重
    private List<EsDoc> readExcelAndDeduplicate(String path, String sheetName) throws Exception {
        InputStream inputStream = new FileInputStream(path);
        Workbook workbook = new XSSFWorkbook(inputStream);
        Sheet sheet = workbook.getSheet(sheetName);

        Set<String> uniqueContents = new HashSet<>();
        List<EsDoc> docs = new ArrayList<>();

        int id = 1;
        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (row == null) continue;

            String title = getCellString(row.getCell(3));  // 第4列：title
            String content = getCellString(row.getCell(1)); // 第2列：content

            if (content == null || content.trim().isEmpty() || uniqueContents.contains(content)) {
                continue;
            }
            uniqueContents.add(content);

            EsDoc doc = new EsDoc();
            doc.setIndex(id++);
            doc.setTitle(title != null ? title : "");
            doc.setContent(content);
            docs.add(doc);
        }

        // 导出 Excel（可选）
        File outFile = new File("处理后的文件.xlsx");
        if (!outFile.exists()) {
            exportToExcel(docs, outFile);
            System.out.println("✅ 已导出去重后 Excel 文件");
        } else {
            System.out.println("⚠️ 文件已存在，跳过导出");
        }

        return docs;
    }

    private void createIndexIfNotExists() throws IOException {
        boolean exists = esClient.indices().exists(e -> e.index(INDEX_NAME)).value();
        if (!exists) {
            esClient.indices().create(CreateIndexRequest.of(c -> c
                    .index(INDEX_NAME)
                    .settings(s -> s.analysis(a -> a
                            .analyzer("ik_max_word_analyzer", aa -> aa
                                    .custom(ca -> ca.tokenizer("ik_max_word"))
                            )))
                    .mappings(m -> m
                            .properties("title", Property.of(p -> p.text(t -> t
                                    .analyzer("ik_max_word")
                                    .searchAnalyzer("ik_smart")
                            )))
                            .properties("content", Property.of(p -> p.text(t -> t
                                    .analyzer("ik_max_word")
                                    .searchAnalyzer("ik_smart")
                            )))
                            .properties("index", Property.of(p -> p.integer(i -> i)))
                    )
            ));
            System.out.println("✅ 已创建索引并启用 IK 分词器");
        } else {
            System.out.println("⚠️ 索引已存在，跳过创建");
        }
    }

    private void exportToExcel(List<EsDoc> docs, File file) throws IOException {
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("去重后数据");

        Row header = sheet.createRow(0);
        header.createCell(0).setCellValue("index");
        header.createCell(1).setCellValue("title");
        header.createCell(2).setCellValue("content");

        int rowNum = 1;
        for (EsDoc doc : docs) {
            Row row = sheet.createRow(rowNum++);
            row.createCell(0).setCellValue(doc.getIndex());
            row.createCell(1).setCellValue(doc.getTitle());
            row.createCell(2).setCellValue(doc.getContent());
        }

        try (FileOutputStream fos = new FileOutputStream(file)) {
            workbook.write(fos);
        }
        workbook.close();
    }

    private String getCellString(Cell cell) {
        if (cell == null) return null;
        return cell.getCellType() == CellType.STRING ? cell.getStringCellValue()
                : cell.getCellType() == CellType.NUMERIC ? String.valueOf(cell.getNumericCellValue())
                : null;
    }

    // 静态内部类用于文档结构
    public static class EsDoc {
        private int index;
        private String title;
        private String content;

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }
    }
}

