package com.demo.demo.properties;

import lombok.Data;

@Data
public class WriterConfig {
  private String type; // DB | FILE | COMPOSITE
  private DbWriterConfig dbWriter;
  private DelimitedFileWriterConfig delimitedFileWriter;
}
