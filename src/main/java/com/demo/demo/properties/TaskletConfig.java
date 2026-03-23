package com.demo.demo.properties;

import lombok.Data;

@Data
public class TaskletConfig {

  private String connectionName;
  private String sql;
  private String jdbc;
  private String sqlScript;
  private String strategy = "simple";
  private int maxThreads = 1;
}
