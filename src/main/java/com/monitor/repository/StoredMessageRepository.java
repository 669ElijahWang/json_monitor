package com.monitor.repository;

import com.monitor.entity.StoredMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.time.Instant;
import java.util.List;

public interface StoredMessageRepository extends JpaRepository<StoredMessage, Long>, JpaSpecificationExecutor<StoredMessage> {
    List<StoredMessage> findByTaskIdOrderByCreatedAtAsc(String taskId);

    long countByCreatedAtBetween(Instant from, Instant to);

    long countByResultAndCreatedAtBetween(String result, Instant from, Instant to);

    List<StoredMessage> findByCreatedAtBetween(Instant from, Instant to);
}
