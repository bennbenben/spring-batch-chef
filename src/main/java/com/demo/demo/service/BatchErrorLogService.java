package com.demo.demo.service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.demo.demo.model.BatchErrorDto;
import com.demo.demo.repository.BatchErrorRepository;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class BatchErrorLogService {

  // Removed 'static' and 'final' so it can be managed by the Spring bean lifecycle cleanly
  private ExecutorService executorService;
  private final BlockingQueue<BatchErrorDto> logQueue;
  
  @Autowired
  private BatchErrorRepository batchErrorRepository;

  // Added a toggle flag. It defaults to true if not found in YAML.
  @Value("${app.batch-error-logger.enabled:true}")
  private boolean isLoggingEnabled;

  public BatchErrorLogService() {
    logQueue = new LinkedBlockingQueue<>();
  }

  public void logError(BatchErrorDto errorDto) {
      if (isLoggingEnabled) {
          logQueue.offer(errorDto);
      }
  }

  @PostConstruct
  private void startQueueConsumer() {
      if (!isLoggingEnabled) {
          return; // Instantly exit during tests if disabled
      }

      // Utilizing Java 21 Virtual Threads!
      executorService = Executors.newSingleThreadExecutor(
              Thread.ofVirtual().name("batch-error-logger").factory()
      );

      executorService.submit(() -> {
          while (!Thread.currentThread().isInterrupted()) {
              try {
                  // Virtual thread yields back to the OS here while waiting!
                  BatchErrorDto errorToSave = logQueue.take();
                  batchErrorRepository.save(errorToSave);
              } catch (InterruptedException e) {
                  Thread.currentThread().interrupt(); // Preserve interrupt status
              } catch (Exception e) {
                  e.printStackTrace(); 
              }
          }
      });
  }

  @PreDestroy
  private void shutdown() {
      if (executorService != null) {
          executorService.shutdownNow(); // Ensure tests kill the thread cleanly
      }
  }
}