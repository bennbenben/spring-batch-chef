package com.demo.demo.properties;

import java.util.List;

import lombok.Data;

@Data
public class ChunkConfig {

  private Integer chunkSize = 10;
  private Integer skipLimit = 10;
  private String strategy = "simple"; // simple | multi-thread | partitioning
  
  // tuning parameters for advanced strategies: multi-thread, partitioning
  private int maxThreads = 4;
  private int gridSize = 4;
  
  private List<String> skippableExceptions;
  private List<String> fatalExceptions;
}
