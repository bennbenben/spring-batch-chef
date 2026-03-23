package com.demo.demo.properties;

import java.util.List;

import lombok.Data;

@Data
public class DelimitedFileWriterConfig {
  private String fileName;
  private String delimiter;
  private String basePath;
  private String headerExpval;
  private List<FieldConfig> fileFields;
}
