package com.example.util;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class EmbeddingUtil {

    @Value("${openai.api.url}")
    private String embeddingApiUrl;
    @Value("${openai.api.key}")
    private String embeddingApiKey;
    public  List<Float> getEmbedding(String text) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(embeddingApiUrl);
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Authorization", "Bearer " + embeddingApiKey);

            JSONObject payload = new JSONObject();
            payload.set("input", text);
            payload.set("model", "bge-large-zh-v1.5");

            post.setEntity(new StringEntity(payload.toString(), "UTF-8"));
            CloseableHttpResponse response = client.execute(post);
            String result = EntityUtils.toString(response.getEntity(), "UTF-8");

            JSONObject json = JSONUtil.parseObj(result);
            JSONArray embeddingArray = json.getJSONArray("data")
                    .getJSONObject(0)
                    .getJSONArray("embedding");

            List<Float> vector = new ArrayList<>();
            for (Object num : embeddingArray) {
                vector.add(Float.parseFloat(num.toString()));
            }
            return vector;
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }
}
