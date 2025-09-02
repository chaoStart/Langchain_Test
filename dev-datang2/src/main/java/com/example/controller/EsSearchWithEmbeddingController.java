package com.example.controller;

import com.example.dto.SearchResponseDto;
import com.example.service.EsSearchWithEmbeddedService;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/search")
public class EsSearchWithEmbeddingController {

    private final EsSearchWithEmbeddedService searchService;

    public EsSearchWithEmbeddingController(EsSearchWithEmbeddedService searchService) {
        this.searchService = searchService;
    }

    @GetMapping
    public List<SearchResponseDto> search(@RequestParam String query) throws IOException {
        return searchService.searchWithEmbedding(query);
    }
}
