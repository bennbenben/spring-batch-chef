package com.demo.demo.config;

import java.util.List;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;

public class DynamicJobFactoryBean implements FactoryBean<Job> {
  
  private String jobName;
  private List<Step> steps; // Updated to handle multiple steps
  
  @Autowired
  private JobRepository jobRepository;

  public void setJobName(String jobName) { this.jobName = jobName; }
  public void setSteps(List<Step> steps) { this.steps = steps; }

  @Override
  public Job getObject() throws Exception {
      if (steps == null || steps.isEmpty()) {
          throw new IllegalArgumentException("Job must have at least one step.");
      }

      JobBuilder jobBuilder = new JobBuilder(jobName, jobRepository);
      SimpleJobBuilder flowBuilder = jobBuilder.start(steps.get(0));
      
      for (int i = 1; i < steps.size(); i++) {
          flowBuilder = flowBuilder.next(steps.get(i));
      }

      return flowBuilder.build();
  }

  @Override
  public Class<?> getObjectType() { return Job.class; }
  @Override
  public boolean isSingleton() { return true; }
}