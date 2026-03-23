package com.demo.demo.traditionalledgerjobconfig;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.mapping.PassThroughLineMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class TraditionalLedgerJobConfig {

  // 1. Define the Job
  @Bean
  public Job traditionalLedgerJob(JobRepository jobRepository, Step traditionalStep1) {
    return new JobBuilder("TraditionalLedgerJob", jobRepository).start(traditionalStep1).build();
  }

  // 2. Define the Step
  @Bean
  public Step traditionalStep1(JobRepository jobRepository, PlatformTransactionManager transactionManager,
      ItemReader<String> traditionalReader, ItemWriter<String> traditionalWriter) {
    return new StepBuilder("traditionalStep1", jobRepository).<String, String>chunk(10, transactionManager)
        .reader(traditionalReader).writer(traditionalWriter).build();
  }

  // 3. THE MAGIC: The Step-Scoped Reader mapping the Job Parameter
  @Bean
  @StepScope
  public ItemReader<String> traditionalReader(@Value("#{jobParameters['fileNameList']}") String fileNameList) {

    log.info("CHEF LOG: Received traditional job parameter fileNameList = {}", fileNameList);

    // Since it is pipe-separated, you can split it right here!
    // For this simple example, we'll just grab the first file in the list.
    String firstFile = "";
    if (fileNameList != null && !fileNameList.isEmpty()) {
      String[] files = fileNameList.split("\\|");
      firstFile = files[0].trim();
    }

    // Build the standard Spring Batch Reader
    return new FlatFileItemReaderBuilder<String>().name("traditionalFileReader")
        .resource(new FileSystemResource("/Data/" + firstFile)).lineMapper(new PassThroughLineMapper()).build();
  }

  // 4. A simple writer for testing
  @Bean
  @StepScope
  public ItemWriter<String> traditionalWriter() {
    return chunk -> {
      for (String item : chunk.getItems()) {
        System.out.println("Writing item: " + item);
      }
    };
  }
}