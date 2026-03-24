package com.demo.demo.config;

import java.io.IOException;
import java.io.Writer;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStreamReader;
import org.springframework.batch.infrastructure.item.ItemStreamWriter;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileHeaderCallback;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.transform.FieldExtractor;
import org.springframework.batch.infrastructure.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.PropertyPlaceholderHelper;

import com.demo.demo.model.BatchErrorDto;
import com.demo.demo.properties.ChunkConfig;
import com.demo.demo.properties.FieldConfig;
import com.demo.demo.properties.ProcessorConfig;
import com.demo.demo.properties.ReaderConfig;
import com.demo.demo.properties.TaskletConfig;
import com.demo.demo.properties.TransformConfig;
import com.demo.demo.properties.WriterConfig;
import com.demo.demo.service.BatchErrorLogService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamicStepFactoryBean implements FactoryBean<Step> {

  private String stepName;
  private String model;
  private TaskletConfig taskletConfig;
  private DataSource dataSource;

  private ChunkConfig chunkConfig;
  private ReaderConfig readerConfig;
  private ProcessorConfig processorConfig;
  private WriterConfig writerConfig;

  // REMOVED: Singleton fileReaderInstance and fileWriterInstance

  @Autowired
  private JobRepository jobRepository;

  @Autowired
  private PlatformTransactionManager transactionManager;

  @Autowired
  private BatchErrorLogService batchErrorLogService;

  @Autowired
  private ApplicationContext applicationContext;

  public void setStepName(String stepName) {
    this.stepName = stepName;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public void setTaskletConfig(TaskletConfig taskletConfig) {
    this.taskletConfig = taskletConfig;
  }

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void setChunkConfig(ChunkConfig chunkConfig) {
    this.chunkConfig = chunkConfig;
  }

  public void setReaderConfig(ReaderConfig readerConfig) {
    this.readerConfig = readerConfig;
  }

  public void setProcessorConfig(ProcessorConfig processorConfig) {
    this.processorConfig = processorConfig;
  }

  public void setWriterConfig(WriterConfig writerConfig) {
    this.writerConfig = writerConfig;
  }

  @Override
  public Step getObject() throws Exception {
    if ("chunk".equalsIgnoreCase(model)) {
      return buildChunkStepRouter();
    } else {
      return buildTaskletStepRouter();
    }
  }

  // ==========================================
  // ROUTER: Decides between Simple, Multi-Thread, or Partition
  // ==========================================
  private Step buildChunkStepRouter() {
    String strategy = chunkConfig.getStrategy() != null ? chunkConfig.getStrategy().toLowerCase() : "simple";

    if ("partition".equals(strategy)) {
      Step workerStep = buildBaseChunkStep(stepName + "-Worker", "simple");

      SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor(stepName + "-Partition-");
      taskExecutor.setConcurrencyLimit(chunkConfig.getMaxThreads());

      return new StepBuilder(stepName, jobRepository).partitioner(workerStep.getName(), buildDynamicFilePartitioner())
          .step(workerStep).taskExecutor(taskExecutor).gridSize(chunkConfig.getGridSize()).build();
    }

    return buildBaseChunkStep(stepName, strategy);
  }

  // ==========================================
  // TASKLET ROUTER & BUILDER (UNTOUCHED)
  // ==========================================
  private Step buildTaskletStepRouter() {
    String strategy = taskletConfig != null && taskletConfig.getStrategy() != null
        ? taskletConfig.getStrategy().toLowerCase()
        : "simple";

    if ("partition".equals(strategy)) {
      Step workerStep = buildBaseTaskletStep(stepName + "-Worker");
      SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor(stepName + "-Partition-");
      taskExecutor.setConcurrencyLimit(taskletConfig.getMaxThreads());

      return new StepBuilder(stepName, jobRepository).partitioner(workerStep.getName(), buildDynamicFilePartitioner())
          .step(workerStep).taskExecutor(taskExecutor).build();
    }

    return buildBaseTaskletStep(stepName);
  }

  private Step buildBaseTaskletStep(String name) {
    Tasklet tasklet = (contribution, chunkContext) -> {
      JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
      String currentSql = taskletConfig.getSql();

      // --- ADD THIS NEW BLOCK RIGHT HERE ---
      // Dynamically replace ANY ${variable} found in the SQL string from the Job
      // Parameters
      Map<String, Object> jobParams = chunkContext.getStepContext().getJobParameters();
      for (Map.Entry<String, Object> entry : jobParams.entrySet()) {
        if (entry.getValue() != null) {
          currentSql = currentSql.replace("${" + entry.getKey() + "}", entry.getValue().toString());
        }
      }

      String fileToProcess = chunkContext.getStepContext().getStepExecution().getExecutionContext()
          .getString("partitionedFileName", null);

      if (fileToProcess == null) {
        Object rawParam = chunkContext.getStepContext().getJobParameters().get("fileNameList");
        if (rawParam != null) {
          fileToProcess = rawParam.toString();
        }
      }

      if (fileToProcess != null && !fileToProcess.trim().isEmpty()) {
        currentSql = currentSql.replace("${fileName}", fileToProcess.trim());
      }

      log.info("CHEF LOG: Executing SQL -> {}", currentSql);

      if ("update".equalsIgnoreCase(taskletConfig.getJdbc())) {
        int rowsAffected = jdbcTemplate.update(currentSql);
        contribution.incrementWriteCount(rowsAffected);

        ExecutionContext stepContext = chunkContext.getStepContext().getStepExecution().getExecutionContext();

        if (fileToProcess != null) {
          stepContext.putString("sourceFileName", fileToProcess.trim());
        }

        try {
          List<String> warnings = jdbcTemplate.queryForList("SHOW WARNINGS", String.class);
          if (warnings != null && !warnings.isEmpty()) {
            stepContext.putString("loadDataWarnings", warnings.toString());
          }
        } catch (Exception e) {
          log.warn("CHEF LOG: Could not fetch MariaDB warnings for file: {}", fileToProcess);
        }
      } else {
        jdbcTemplate.execute(currentSql);
      }

      return RepeatStatus.FINISHED;
    };

    return new StepBuilder(name, jobRepository).tasklet(tasklet, transactionManager).build();
  }

  // ==========================================
  // BASE CHUNK STEP BUILDER
  // ==========================================
  @SuppressWarnings("unchecked")
  private Step buildBaseChunkStep(String name, String strategy) {
    ItemReader<Map<String, Object>> rawReader = buildReader();

    ItemReader<Map<String, Object>> threadSafeReader = rawReader;
    if ("multi-thread".equals(strategy)) {
      threadSafeReader = new SynchronizedItemStreamReaderBuilder<Map<String, Object>>()
          .delegate((ItemStreamReader<Map<String, Object>>) rawReader).build();
    }

    ItemWriter<Map<String, Object>> writer = buildWriter();

    // 1. initialize the base Step builder
    var faultTolerantBuilder = new StepBuilder(name, jobRepository)
        .<Map<String, Object>, Map<String, Object>>chunk(chunkConfig.getChunkSize(), transactionManager)
        .reader(threadSafeReader).processor(buildProcessor()).writer(writer)
        .faultTolerant()
        .skipLimit(chunkConfig.getSkipLimit()).listener(buildSkipListener());

    // 2. Dynamically add Skippable Exceptions from YAML
    if (chunkConfig.getSkippableExceptions() != null && !chunkConfig.getSkippableExceptions().isEmpty()) {
      for (String className : chunkConfig.getSkippableExceptions()) {
        try {
          Class<? extends Throwable> exceptionClass = (Class<? extends Throwable>) Class.forName(className);
          faultTolerantBuilder.skip(exceptionClass);
        } catch (ClassNotFoundException e) {
          log.error("CHEF LOG: Invalid skippable exception class in YAML: {}", className);
        }
      }
    }

    // 3. Dynamically add Fatal Exceptions (No-Skip) from YAML
    if (chunkConfig.getFatalExceptions() != null && !chunkConfig.getFatalExceptions().isEmpty()) {
      for (String className : chunkConfig.getFatalExceptions()) {
        try {
          Class<? extends Throwable> exceptionClass = (Class<? extends Throwable>) Class.forName(className);
          faultTolerantBuilder.noSkip(exceptionClass);
        } catch (ClassNotFoundException e) {
          log.error("CHEF LOG: Invalid fatal exception class in YAML: {}", className);
        }
      }
    }

    // 4. Attach the final Execution Listener and build
    faultTolerantBuilder.listener(buildStepExecutionListener());

    if ("multi-thread".equals(strategy)) {
      SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor(name + "-Thread-");
      taskExecutor.setConcurrencyLimit(chunkConfig.getMaxThreads());
      return faultTolerantBuilder.taskExecutor(taskExecutor).build();
    }

    return faultTolerantBuilder.build();
  }

  // ==========================================
  // DYNAMIC FILE PARTITIONER
  // ==========================================
  private Partitioner buildDynamicFilePartitioner() {
    return gridSize -> {
      Map<String, ExecutionContext> partitions = new HashMap<>();
      var jobParams = StepSynchronizationManager.getContext().getStepExecution().getJobParameters();
      String rawFileNameList = null;

      if (readerConfig != null && "FILE".equalsIgnoreCase(readerConfig.getType())) {
        rawFileNameList = readerConfig.getDelimitedFileReader().getFileNameList();
      } else if (taskletConfig != null) {
        rawFileNameList = "${fileNameList}";
      }

      if (rawFileNameList != null) {
        PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}");
        String resolvedFileList = helper.replacePlaceholders(rawFileNameList, placeholderName -> {
          String paramValue = jobParams.getString(placeholderName);
          return paramValue != null ? paramValue : "";
        });

        if (resolvedFileList != null && !resolvedFileList.isEmpty()) {
          String[] files = resolvedFileList.split("\\|");
          for (int i = 0; i < files.length; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putString("partitionedFileName", files[i].trim());
            String fileName = files[i].trim();
            String safePartitionName = fileName.replaceAll("[^a-zA-Z0-9_\\-\\.]", "_");
            partitions.put(safePartitionName, context);
          }
        }
      }
      return partitions;
    };
  }

  // ==========================================
  // STEP EXECUTION LISTENER
  // ==========================================
  private StepExecutionListener buildStepExecutionListener() {
    return new StepExecutionListener() {
      @Override
      public void beforeStep(StepExecution stepExecution) {
        // Resource assignment logic has been safely moved to the ThreadLocal
        // Reader and Writer implementations to prevent Singleton Race Conditions!
      }
    };
  }

  // ==========================================
  // SKIP LISTENER (Error Logging)
  // ==========================================
  private SkipListener<Map<String, Object>, Map<String, Object>> buildSkipListener() {
    return new SkipListener<Map<String, Object>, Map<String, Object>>() {
      @Override
      public void onSkipInRead(Throwable t) {
        log.error("Error found onSkipInRead:{}", t);
//        logBatchError(null, "READER_ERROR", t.getMessage());
      }

      @Override
      public void onSkipInProcess(Map<String, Object> item, Throwable t) {
        log.error("Error found onSkipInProcess:{}", t);
//        logBatchError(item, "PROCESSOR_ERROR", t.getMessage());
      }

      @Override
      public void onSkipInWrite(Map<String, Object> item, Throwable t) {
        log.error("Error found onSkipInWrite:{}", t);
        // logBatchError(item, "WRITER_ERROR", t.getMessage());
      }

      private void logBatchError(Map<String, Object> item, String status, String errorMsg) {
        StepExecution stepExecution = StepSynchronizationManager.getContext().getStepExecution();
        BatchErrorDto errorDto = new BatchErrorDto();
        errorDto.setJobExecutionId(stepExecution.getJobExecutionId());
        errorDto.setJobName(stepExecution.getJobExecution().getJobInstance().getJobName());

        String target = "DB".equalsIgnoreCase(writerConfig.getType()) ? writerConfig.getDbWriter().getSql()
            : writerConfig.getDelimitedFileWriter().getFileName();
        errorDto.setTargetTable(target);

        errorDto.setStatus(status);
        errorDto.setErrDesc(errorMsg != null && errorMsg.length() > 250 ? errorMsg.substring(0, 250) : errorMsg);
        errorDto.setCreatedTs(LocalDateTime.now());
        if (item != null) {
          errorDto.setRowData(item.toString());
        }

        batchErrorLogService.logError(errorDto);
      }
    };
  }

//==========================================
  // DYNAMIC THREAD-SAFE READER
  // ==========================================
  private ItemReader<Map<String, Object>> buildReader() {
    if ("DB".equalsIgnoreCase(readerConfig.getType())) {

      // ThreadLocal Wrapper ensures every thread builds and uses its own private DB
      // Cursor!
      return new ItemStreamReader<Map<String, Object>>() {
        private final ThreadLocal<ItemStreamReader<Map<String, Object>>> threadLocalDbReader = new ThreadLocal<>();

        @Override
        public void open(ExecutionContext executionContext) {
          DataSource ds = applicationContext.getBean(readerConfig.getDbReader().getConnectionName(), DataSource.class);

          // Build a brand new DB Cursor for whoever called this method
          JdbcCursorItemReader<Map<String, Object>> isolatedDbReader = new JdbcCursorItemReaderBuilder<Map<String, Object>>()
              .name(StepSynchronizationManager.getContext().getStepExecution().getStepName() + "-DbReader")
              .dataSource(ds).sql(readerConfig.getDbReader().getSql()).rowMapper(new ColumnMapRowMapper())
              .verifyCursorPosition(false).connectionAutoCommit(false).build();

          isolatedDbReader.open(executionContext);
          threadLocalDbReader.set(isolatedDbReader);
        }

        @Override
        public Map<String, Object> read() throws Exception {
          return threadLocalDbReader.get().read();
        }

        @Override
        public void update(ExecutionContext executionContext) {
          if (threadLocalDbReader.get() != null)
            threadLocalDbReader.get().update(executionContext);
        }

        @Override
        public void close() {
          if (threadLocalDbReader.get() != null) {
            threadLocalDbReader.get().close();
            threadLocalDbReader.remove(); // Clean up the connection and vault!
          }
        }
      };

    } else {

      // ThreadLocal Wrapper ensures every thread builds and uses its own private File
      // Reader!
      return new ItemStreamReader<Map<String, Object>>() {
        private final ThreadLocal<ItemStreamReader<Map<String, Object>>> threadLocalReader = new ThreadLocal<>();

        @Override
        public void open(ExecutionContext executionContext) {
          StepExecution stepExecution = StepSynchronizationManager.getContext().getStepExecution();
          String partitionedFile = stepExecution.getExecutionContext().getString("partitionedFileName", null);
          PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}");

          // --- ADD THIS NEW BLOCK ---
          String rawBasePath = readerConfig.getDelimitedFileReader().getBasePath();
          String resolvedBasePath = helper.replacePlaceholders(rawBasePath != null ? rawBasePath : "",
              placeholderName -> {
                String paramValue = stepExecution.getJobParameters().getString(placeholderName);
                return paramValue != null ? paramValue : "";
              });
          // --------------------------

          ItemStreamReader<Map<String, Object>> isolatedReader;

          if (partitionedFile != null) {
            // WORKER THREAD: Read specific assigned file
            String fullPath = resolvedBasePath + partitionedFile;
            isolatedReader = createFlatFileReader(stepExecution.getStepName() + "-ThreadReader",
                new FileSystemResource(fullPath));
          } else {
            // SIMPLE STRATEGY: Read all files in the parameter list using MultiResource
            String rawFileNameList = readerConfig.getDelimitedFileReader().getFileNameList();
            String resolvedFileList = "";
            if (rawFileNameList != null) {
              resolvedFileList = helper.replacePlaceholders(rawFileNameList, placeholderName -> {
                String paramValue = stepExecution.getJobParameters().getString(placeholderName);
                return paramValue != null ? paramValue : "";
              });
            }

            String[] files = resolvedFileList.split("\\|");
            Resource[] resources = new Resource[files.length];
            for (int i = 0; i < files.length; i++) {
              String fullPath = resolvedBasePath + files[i].trim();
              resources[i] = new FileSystemResource(fullPath);
            }

            isolatedReader = new MultiResourceItemReaderBuilder<Map<String, Object>>()
                .name(stepExecution.getStepName() + "-MultiReader")
                .delegate(createFlatFileReader(stepExecution.getStepName() + "-Delegate", null)).resources(resources)
                .build();
          }

          isolatedReader.open(executionContext);
          threadLocalReader.set(isolatedReader);
        }

        @Override
        public Map<String, Object> read() throws Exception {
          return threadLocalReader.get().read();
        }

        @Override
        public void update(ExecutionContext executionContext) {
          if (threadLocalReader.get() != null)
            threadLocalReader.get().update(executionContext);
        }

        @Override
        public void close() {
          if (threadLocalReader.get() != null) {
            threadLocalReader.get().close();
            threadLocalReader.remove(); // Clear vault to prevent memory leaks
          }
        }
      };
    }
  }

  // Helper method to build the base FlatFileItemReader logic
  private FlatFileItemReader<Map<String, Object>> createFlatFileReader(String name, Resource resource) {
    String[] columnNames = readerConfig.getDelimitedFileReader().getFileFields().stream().map(FieldConfig::getName)
        .toArray(String[]::new);

    FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>()
        .name(name).delimited().delimiter(readerConfig.getDelimitedFileReader().getDelimiter()).names(columnNames)
        .fieldSetMapper(fieldSet -> {
          Properties props = fieldSet.getProperties();
          Map<String, Object> map = new HashMap<>();
          for (String key : props.stringPropertyNames()) {
            map.put(key, props.getProperty(key));
          }
          return map;
        });

    if (resource != null) {
      builder.resource(resource);
    }
    return builder.build();
  }

  // ==========================================
  // PROCESSOR
  // ==========================================
  private ItemProcessor<Map<String, Object>, Map<String, Object>> buildProcessor() {

    // 1. Pass-through if no processor block exists at all
    if (processorConfig == null) {
      return item -> item;
    }

    // 2. THE NEW HYBRID HOOK: Fetch the custom Java bean directly from Spring!
    if ("custom-bean".equalsIgnoreCase(processorConfig.getType()) && processorConfig.getBeanName() != null) {
      log.info("CHEF LOG: Injecting custom processor bean: {}", processorConfig.getBeanName());
      return applicationContext.getBean(processorConfig.getBeanName(), ItemProcessor.class);
    }

    // 3. Pass-through if it's dynamic but no transforms are defined
    if (processorConfig.getTransforms() == null || processorConfig.getTransforms().isEmpty()) {
      return item -> item;
    }

    // 4. THE EXISTING DYNAMIC LOGIC
    return item -> {
      Map<String, Object> processedItem = new HashMap<>();
      for (TransformConfig transform : processorConfig.getTransforms()) {
        if ("direct-map".equalsIgnoreCase(transform.getType()) || "custom".equalsIgnoreCase(transform.getType())) {
          Object value = item.get(transform.getReaderField());
          boolean isNullOrEmpty = (value == null) || (value instanceof String && ((String) value).isEmpty());
          // only trigger replacement if yaml contains a default-value property
          if (isNullOrEmpty && transform.getDefaultValue() != null) {
            // check for reserved keyword: {NULL}
            if ("{NULL}".equals(transform.getDefaultValue())) {
              value = new SqlParameterValue(Types.VARCHAR, null);
            } else {
              value = transform.getDefaultValue();
            }
          }
          processedItem.put(transform.getWriterField(), value);
        }
      }
      return processedItem;
    };
  }

  // ==========================================
  // DYNAMIC THREAD-SAFE WRITER
  // ==========================================
  private ItemWriter<Map<String, Object>> buildWriter() {
    if ("FILE".equalsIgnoreCase(writerConfig.getType())) {

      // ThreadLocal Wrapper ensures concurrent workers write to their own isolated
      // files!
      return new ItemStreamWriter<Map<String, Object>>() {
        private final ThreadLocal<FlatFileItemWriter<Map<String, Object>>> threadLocalWriter = new ThreadLocal<>();

        @Override
        public void open(ExecutionContext executionContext) {
          StepExecution stepExecution = StepSynchronizationManager.getContext().getStepExecution();
          PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}");

          String rawFileName = writerConfig.getDelimitedFileWriter().getFileName();
          String resolvedFileName = "";
          if (rawFileName != null) {
            resolvedFileName = helper.replacePlaceholders(rawFileName, placeholderName -> {
              String paramValue = stepExecution.getJobParameters().getString(placeholderName);
              return paramValue != null ? paramValue : "";
            });
          }

          // --- ADD THIS NEW BLOCK ---
          String rawBasePath = writerConfig.getDelimitedFileWriter().getBasePath();
          String resolvedBasePath = helper.replacePlaceholders(rawBasePath != null ? rawBasePath : "",
              placeholderName -> {
                String paramValue = stepExecution.getJobParameters().getString(placeholderName);
                return paramValue != null ? paramValue : "";
              });
          // --------------------------

          String fullPath = resolvedBasePath + resolvedFileName.trim();

          // Append partition name so workers don't overwrite each other!
          if ("partition".equalsIgnoreCase(chunkConfig.getStrategy())) {
            fullPath = fullPath.replace(".csv", "_" + stepExecution.getStepName() + ".csv");
          }

          String[] columnNames = writerConfig.getDelimitedFileWriter().getFileFields().stream()
              .map(FieldConfig::getName).toArray(String[]::new);

          FlatFileItemWriter<Map<String, Object>> isolatedWriter = new FlatFileItemWriterBuilder<Map<String, Object>>()
              .name(stepExecution.getStepName() + "-FileWriter").resource(new FileSystemResource(fullPath)).delimited()
              .delimiter(writerConfig.getDelimitedFileWriter().getDelimiter())
              .fieldExtractor(new FieldExtractor<Map<String, Object>>() {
                @Override
                public Object[] extract(Map<String, Object> item) {
                  Object[] values = new Object[columnNames.length];
                  for (int i = 0; i < columnNames.length; i++) {
                    values[i] = item.get(columnNames[i]);
                  }
                  return values;
                }
              }).build();

          if (writerConfig.getDelimitedFileWriter().getHeaderExpval() != null) {
            isolatedWriter.setHeaderCallback(new FlatFileHeaderCallback() {

              @Override
              public void writeHeader(Writer writer) throws IOException {
                writer.write(writerConfig.getDelimitedFileWriter().getHeaderExpval());
              }
            });
          }

          isolatedWriter.open(executionContext);
          threadLocalWriter.set(isolatedWriter);
        }

        @Override
        public void write(Chunk<? extends Map<String, Object>> chunk) throws Exception {
          threadLocalWriter.get().write(chunk);
        }

        @Override
        public void update(ExecutionContext executionContext) {
          if (threadLocalWriter.get() != null)
            threadLocalWriter.get().update(executionContext);
        }

        @Override
        public void close() {
          if (threadLocalWriter.get() != null) {
            threadLocalWriter.get().close();
            threadLocalWriter.remove(); // Clear vault
          }
        }
      };
    } else {
      DataSource ds = applicationContext.getBean(writerConfig.getDbWriter().getConnectionName(), DataSource.class);
      return new JdbcBatchItemWriterBuilder<Map<String, Object>>().dataSource(ds)
          .sql(writerConfig.getDbWriter().getSql()).itemSqlParameterSourceProvider(MapSqlParameterSource::new).build();
    }
  }

  @Override
  public Class<?> getObjectType() {
    return Step.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }
}