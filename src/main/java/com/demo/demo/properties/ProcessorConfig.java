package com.demo.demo.properties;

import java.util.List;

import lombok.Data;

@Data
public class ProcessorConfig {
  private String type; 
  private String beanName;
  private List<TransformConfig> transforms;
}
