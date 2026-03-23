package com.demo.demo.model;

import java.util.HashMap;

import lombok.Data;

@Data
public class RunJobRequestDto {
  private String jobName;
  private HashMap<String, String> jobParameters;
}
