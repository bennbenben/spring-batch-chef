package com.demo.demo.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import lombok.Data;

@Entity
@Data
public class BatchErrorDto {

  private static final long serialVersionUID = 1L;
  
  @Id
  @SequenceGenerator(name = "batchError_seq_pooled_generator", sequenceName = "seq_batch_error_id", allocationSize = 1)
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "batchError_seq_pooled_generator")
  private Long id;
  
  @Column(name = "job_execution_id")
  private Long jobExecutionId;
  @Column(name = "job_name")
  private String jobName;
  @Column(name = "file_name")
  private String fileName;
  @Column(name = "row_no")
  private Long rowNo;
  @Column(name = "row_data")
  private String rowData;
  @Column(name = "target_table")
  private String targetTable;
  @Column(name = "status")
  private String status;
  @Column(name = "err_desc")
  private String errDesc;
  @Column(name = "created_ts")
  private LocalDateTime createdTs;
}
