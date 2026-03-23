package com.demo.demo.model;

import java.time.LocalDateTime;

import lombok.Data;

@Data
public class StepProperties {
  private Long stepExecutionId;
  private String stepName;
  private LocalDateTime createTime;
  private LocalDateTime startTime;
  private LocalDateTime endTime;
  private String status;
  private long readCount;
  private long filterCount;
  private long writeCount;
  private long readSkipCount;
  private long processSkipCount;
  private long writeSkipCount;
  private long commitCount;
  private long rollbackCount;
  private String exitStatus;
}
