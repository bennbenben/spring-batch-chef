package com.demo.demo.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class VerifyJobResponseDto {
  private Long jobId;
  private String jobName;
  private String jobStatus;
  private int status;
  private String errorMsg;
  private HashMap<String, String> jobParameters;
  private Map<String, StepProperties> batchStepExecution;
  private List<String> grafanaLogs;
  private int errorCount;
}
