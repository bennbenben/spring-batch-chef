package com.demo.demo.config;

import java.io.InputStream;

import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.AnnotationMetadata;

import com.demo.demo.properties.JobRecipe;
import com.demo.demo.properties.StepConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChefJobRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

  private Environment environment;
  private final ObjectMapper yamlMapper;

  public ChefJobRegistrar() {
    this.yamlMapper = new ObjectMapper(new YAMLFactory());
    this.yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
    this.yamlMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
  }

  @Override
  public void setEnvironment(Environment environment) {
    this.environment = environment;
  }

  public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
    ChefProperties chefProperties = Binder.get(environment).bind("spring-batch-chef", ChefProperties.class)
        .orElse(new ChefProperties());

    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    try {
      for (String path : chefProperties.getRecipePaths()) {
        Resource[] resources = resolver.getResources(path);
        for (Resource resource : resources) {
          JobRecipe jobRecipe = this.parseRecipe(resource);
          if (jobRecipe != null) {
            log.info("Spring Batch Chef: Successfully registered job bean = {}", jobRecipe.getJobName());
            this.cookAndRegister(jobRecipe, registry);
          } else {
            log.error("Spring Batch Chef: Failed to parse recipe file = {}", resource.getFilename());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Spring batch chef did not resolve to any recipe paths", e);
    }

  }

  private JobRecipe parseRecipe(Resource resource) {
    try (InputStream inputStream = resource.getInputStream()) {
      return yamlMapper.readValue(inputStream, JobRecipe.class);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private void cookAndRegister(JobRecipe jobRecipe, BeanDefinitionRegistry registry) {
    ManagedList<RuntimeBeanReference> stepReferences = new ManagedList<>();

    // 1. Register each Step
    for (StepConfig stepConfig : jobRecipe.getSteps()) {
      String stepBeanName = jobRecipe.getJobName() + "-" + stepConfig.getStepName();

      BeanDefinitionBuilder stepBuilder = BeanDefinitionBuilder.genericBeanDefinition(DynamicStepFactoryBean.class);
      stepBuilder.addPropertyValue("stepName", stepConfig.getStepName());
      stepBuilder.addPropertyValue("model", stepConfig.getModel());

      if ("tasklet".equalsIgnoreCase(stepConfig.getModel()) && stepConfig.getTasklet() != null) {
        stepBuilder.addPropertyValue("taskletConfig", stepConfig.getTasklet());
        stepBuilder.addPropertyReference("dataSource", stepConfig.getTasklet().getConnectionName());
      }
      // NEW: Handle the Chunk configuration mapping
      else if ("chunk".equalsIgnoreCase(stepConfig.getModel())) {
        stepBuilder.addPropertyValue("chunkConfig", stepConfig.getChunk());
        stepBuilder.addPropertyValue("readerConfig", stepConfig.getReader());
        stepBuilder.addPropertyValue("processorConfig", stepConfig.getProcessor());
        stepBuilder.addPropertyValue("writerConfig", stepConfig.getWriter());

        // SAFE CHECK: Only inject the DB connection if the writer is actually a
        // database!
        if (stepConfig.getWriter() != null && "DB".equalsIgnoreCase(stepConfig.getWriter().getType())
            && stepConfig.getWriter().getDbWriter() != null) {
          stepBuilder.addPropertyReference("dataSource", stepConfig.getWriter().getDbWriter().getConnectionName());
        }
      }

      registry.registerBeanDefinition(stepBeanName, stepBuilder.getBeanDefinition());
      stepReferences.add(new RuntimeBeanReference(stepBeanName));
    }

    // 2. Register the Job, injecting the list of Step references
    String jobBeanName = jobRecipe.getJobName();
    BeanDefinitionBuilder jobBuilder = BeanDefinitionBuilder.genericBeanDefinition(DynamicJobFactoryBean.class);
    jobBuilder.addPropertyValue("jobName", jobRecipe.getJobName());
    jobBuilder.addPropertyValue("steps", stepReferences);

    registry.registerBeanDefinition(jobBeanName, jobBuilder.getBeanDefinition());
  }

}
