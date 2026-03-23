package com.demo.demo.properties;

import lombok.Data;

@Data
public class StepConfig {

  private String stepName;
  private String model;
  private TaskletConfig tasklet;
  private ChunkConfig chunk;
  private ReaderConfig reader;
  private ProcessorConfig processor;
  private WriterConfig writer;
  
}
