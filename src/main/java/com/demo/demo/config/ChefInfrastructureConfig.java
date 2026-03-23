package com.demo.demo.config;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JdbcJobRepositoryFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import jakarta.persistence.EntityManagerFactory;

@Configuration
@EnableBatchProcessing(taskExecutorRef = "chefTaskExecutor")
@EnableJdbcJobRepository
@Import({DynamicDataSourceRegistrar.class, ChefJobRegistrar.class})
public class ChefInfrastructureConfig {

  
  public ThreadPoolTaskExecutor chefTaskExecutor(ChefProperties chefProperties) {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setCorePoolSize(chefProperties.getConcurrency().getCorePoolSize());
    threadPoolTaskExecutor.setMaxPoolSize(chefProperties.getConcurrency().getMaxPoolSize());
    threadPoolTaskExecutor.setQueueCapacity(chefProperties.getConcurrency().getQueueCapacity());
    threadPoolTaskExecutor.setThreadNamePrefix("chef-async-");
    
    threadPoolTaskExecutor.initialize();
    return threadPoolTaskExecutor;
  }
  
  @Bean
  public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
    return new JpaTransactionManager(entityManagerFactory);
  }
  
  @Bean
  public JobRepository jobRepository(DataSource dataSource, JpaTransactionManager transactionManager, ChefProperties chefProperties) throws Exception {
    JdbcJobRepositoryFactoryBean jdbcJobRepositoryFactoryBean = new JdbcJobRepositoryFactoryBean();
    jdbcJobRepositoryFactoryBean.setDataSource(dataSource);
    jdbcJobRepositoryFactoryBean.setTransactionManager(transactionManager);
    jdbcJobRepositoryFactoryBean.setTablePrefix(chefProperties.getTablePrefix());
    jdbcJobRepositoryFactoryBean.afterPropertiesSet();
    return jdbcJobRepositoryFactoryBean.getObject();
  }
  
  @Bean
  public JobOperator jobOperator(JobRepository jobRepository, JobRegistry jobRegistry, ThreadPoolTaskExecutor chefTaskExecutor) {
    TaskExecutorJobOperator taskExecutorJobOperator = new TaskExecutorJobOperator();
    taskExecutorJobOperator.setJobRepository(jobRepository);
    taskExecutorJobOperator.setJobRegistry(jobRegistry);
    taskExecutorJobOperator.setTaskExecutor(chefTaskExecutor);
    return taskExecutorJobOperator;
  }
  
  @Bean
  public JobRegistry jobRegistry() {
    return new MapJobRegistry();
  }
  
  @Bean
  public DataSourceInitializer batchSchemaInitializer(DataSource dataSource) throws SQLException {
      ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
      String databaseProductName = dataSource.getConnection().getMetaData().getDatabaseProductName();
      
      String schemaScriptPath;
      if (databaseProductName.contains("H2")) {
        schemaScriptPath = "org/springframework/batch/core/schema-h2.sql";
      } else if (databaseProductName.contains("MariaDB") || databaseProductName.contains("MySQL")) {
        schemaScriptPath = "org/springframework/batch/core/schema-mysql.sql";
      } else if (databaseProductName.contains("PostgreSQL")) {
        schemaScriptPath = "org/springframework/batch/core/schema-postgresql.sql";
      } else {
        throw new IllegalStateException(
            "Unsupported database type for Spring Batch schema initialization: " + databaseProductName);
      }
      
      // Load the default H2 schema script provided by Spring Batch
      populator.addScript(new ClassPathResource(schemaScriptPath));
      populator.setContinueOnError(true); // Prevents crashing on restarts if tables already exist

      DataSourceInitializer initializer = new DataSourceInitializer();
      initializer.setDataSource(dataSource);
      initializer.setDatabasePopulator(populator);
      
      return initializer;
  }
}
