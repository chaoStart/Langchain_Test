package com.example.controller;

import com.example.service.SheetSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/sheets")
public class SheetSearchController {

    @Autowired
    private SheetSearchService sheetSearchService;

    @GetMapping("/search")
    public List<String> searchSheetNames(@RequestParam("q") String keywords,
                                         @RequestParam(value = "top_k", defaultValue = "10") int topK) throws IOException {
        return sheetSearchService.searchSheetNames(keywords, topK);
    }

    @GetMapping("/count")
    public long countDocuments() throws IOException {
        return sheetSearchService.countDocuments();
    }
}
