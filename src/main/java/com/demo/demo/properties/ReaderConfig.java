package com.demo.demo.properties;

import lombok.Data;

@Data
public class ReaderConfig {
  private String type; // DB Or FILE
  private DbReaderConfig dbReader;
  private DelimitedFileReaderConfig delimitedFileReader;
}
