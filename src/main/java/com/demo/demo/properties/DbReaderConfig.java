package com.demo.demo.properties;

import java.util.List;

import lombok.Data;

@Data
public class DbReaderConfig {

  private String connectionName;
  private String implementation;
  private String sql;
  private List<FieldConfig> fields;
}
