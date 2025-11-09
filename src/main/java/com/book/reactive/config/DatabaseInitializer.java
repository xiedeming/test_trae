package com.book.reactive.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@Component
public class DatabaseInitializer implements ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    private R2dbcEntityTemplate r2dbcEntityTemplate;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        // try {
        //     // 读取schema.sql文件
        //     String schemaSql = new String(
        //             Files.readAllBytes(Paths.get(new ClassPathResource("schema.sql").getURI())),
        //             StandardCharsets.UTF_8);
            
        //     // 执行SQL脚本
        //     r2dbcEntityTemplate.getDatabaseClient()
        //             .sql(schemaSql)
        //             .then()
        //             .subscribe(
        //                     (Void unused) -> System.out.println("数据库表创建成功"),
        //                     error -> System.err.println("数据库表创建失败: " + error.getMessage())
        //             );
        // } catch (IOException e) {
        //     System.err.println("读取schema.sql文件失败: " + e.getMessage());
        // }
    }
}