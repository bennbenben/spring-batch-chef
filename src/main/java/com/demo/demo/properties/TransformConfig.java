package com.demo.demo.properties;

import lombok.Data;

@Data
public class TransformConfig {

  private String type; // direct-map | custom
  private String readerField;
  private String writerField;
  
  private Object defaultValue;
}
