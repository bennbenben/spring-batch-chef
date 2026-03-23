package com.demo.demo.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "spring-batch-chef")
public class ChefProperties {
  
  private List<String> recipePaths = new ArrayList<String>();
  private int defaultChunkSize=1;
  private Concurrency concurrency = new Concurrency();
  private String tablePrefix = "BATCH_";
  
  @Data
  public static class Concurrency {
    private int corePoolSize = 5;
    private int maxPoolSize = 10;
    private int queueCapacity = 100;
    private String queueBehaviour = "CALLER_RUNS";
  }
}
