package com.demo.demo.properties;

import java.util.List;

import lombok.Data;

@Data
public class DelimitedFileReaderConfig {
  private String implementation;
  private String delimiter;
  private String basePath;
  private String fileNameList;
  private List<FieldConfig> fileFields;
}
