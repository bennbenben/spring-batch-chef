package com.demo.demo.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.demo.demo.model.RunJobRequestDto;
import com.demo.demo.model.RunJobResponseDto;
import com.demo.demo.model.StepProperties;
import com.demo.demo.model.VerifyJobResponseDto;

@RestController
@RequestMapping("/v1/batch")
public class JobController {

  @Autowired
  private JobOperator jobOperator; // Using JobOperator instead of JobLauncher!

  @Autowired
  private JobRepository jobRepository;

  @Autowired
  private JobExplorer jobExplorer; // Needed to fetch metrics after launching

  @PostMapping(value = "/runjob", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<RunJobResponseDto> runJob(@RequestBody RunJobRequestDto runJobRequestDto) {

    try {
      String jobName = runJobRequestDto.getJobName();

      // JobOperator uses standard Java Properties for parameters
      Properties properties = new Properties();
      properties.setProperty("timestamp", String.valueOf(System.currentTimeMillis()));

      if (runJobRequestDto.getJobParameters() != null) {
        runJobRequestDto.getJobParameters().forEach(properties::setProperty);
      }

      // LAUNCH: This executes asynchronously! It returns the Execution ID instantly.
      Long executionId = jobOperator.start(jobName, properties);

      // Fetch the execution from the database using JobExplorer
      JobExecution jobExecution = jobExplorer.getJobExecution(executionId);

      RunJobResponseDto responseDto = new RunJobResponseDto();
      responseDto.setStatus(0);
      responseDto.setJobName(jobName);
      responseDto.setJobId(jobExecution.getId());
      responseDto.setJobStatus(jobExecution.getStatus().name());
      responseDto.setJobParameters(runJobRequestDto.getJobParameters());

      // Map Step metrics (Note: Since it's async, these might be empty initially!)
      Map<String, StepProperties> stepPropertiesMap = new HashMap<>();
      for (StepExecution stepExec : jobExecution.getStepExecutions()) {
        StepProperties stepProps = new StepProperties();
        stepProps.setStepExecutionId(stepExec.getId());
        stepProps.setStepName(stepExec.getStepName());
        stepProps.setStartTime(stepExec.getStartTime());
        stepProps.setEndTime(stepExec.getEndTime());
        stepProps.setStatus(stepExec.getStatus().name());
        stepProps.setReadCount(stepExec.getReadCount());
        stepProps.setWriteCount(stepExec.getWriteCount());
        // ... (map other properties as before)

        stepPropertiesMap.put(stepExec.getStepName(), stepProps);
      }
      responseDto.setBatchStepExecution(stepPropertiesMap);

      return new ResponseEntity<>(responseDto, HttpStatus.OK);

    } catch (Exception e) {
      e.printStackTrace();
      RunJobResponseDto responseDto = new RunJobResponseDto();
      responseDto.setStatus(1);
      responseDto.setErrorMsg(e.getMessage());
      return new ResponseEntity<>(responseDto, HttpStatus.BAD_REQUEST);
    }
  }

  @GetMapping(value = "/verifyJob/{jobId}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<VerifyJobResponseDto> verifyJob(@PathVariable("jobId") Long jobId) {
    VerifyJobResponseDto verifyJobResponseDto = new VerifyJobResponseDto();
    
    try {
      JobExecution jobExecution = jobRepository.getJobExecution(jobId);
      verifyJobResponseDto.setStatus(0);
      verifyJobResponseDto.setJobName(jobExecution.getJobInstance().getJobName());
      verifyJobResponseDto.setJobId(jobExecution.getId());
      verifyJobResponseDto.setJobStatus(jobExecution.getStatus().name());
      return new ResponseEntity<>(verifyJobResponseDto, HttpStatus.OK);
    } catch (Exception e) {
      verifyJobResponseDto.setStatus(1);
      verifyJobResponseDto.setErrorMsg(e.getMessage());
      e.printStackTrace();
      return new ResponseEntity<>(verifyJobResponseDto, HttpStatus.BAD_REQUEST);
    }

  }
}