package com.demo.demo.properties;

import java.util.List;

import lombok.Data;

@Data
public class JobRecipe {

  private String jobName;
  private List<StepConfig> steps;
}
