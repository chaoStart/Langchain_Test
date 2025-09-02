package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication  // 启动注解
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);  // 启动 Spring Boot 项目
    }
}


//public class Main {
//    public static void main(String[] args) {
//        try {
////            String userQuestion = "请问其中煤机的发电量是多少？";
////            Retriever.retrieveByQuestion(userQuestion);
//              QuestionRewriteService.rewriteQuestion();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}