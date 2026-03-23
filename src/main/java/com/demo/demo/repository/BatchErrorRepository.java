package com.demo.demo.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.demo.demo.model.BatchErrorDto;

@Repository
public interface BatchErrorRepository extends JpaRepository<BatchErrorDto, Long> {
}