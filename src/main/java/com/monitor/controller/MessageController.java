package com.monitor.controller;

import com.monitor.entity.StoredMessage;
import com.monitor.repository.StoredMessageRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.persistence.criteria.Predicate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 消息查询接口：
 * - 列表分页
 * - 按 taskId 查询链路消息
 * - 多条件检索（包含 rawJson 的模糊匹配）
 */
@RestController
@RequestMapping("/api")
public class MessageController {
    private final StoredMessageRepository repository;

    public MessageController(StoredMessageRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/messages")
    public Page<StoredMessage> list(@RequestParam(defaultValue = "0") int page,
                                    @RequestParam(defaultValue = "20") int size) {
        return repository.findAll(PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt")));
    }

    @GetMapping("/messages/{taskId}")
    public List<StoredMessage> byTaskId(@PathVariable String taskId) {
        return repository.findByTaskIdOrderByCreatedAtAsc(taskId);
    }

    @GetMapping("/messages/search")
    public Page<StoredMessage> search(@RequestParam(required = false) String q,
                                      @RequestParam(required = false) String tenant,
                                      @RequestParam(required = false) String systemNo,
                                      @RequestParam(required = false) String busId,
                                      @RequestParam(required = false) String taskId,
                                      @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
                                      @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
                                      @RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "20") int size) {
        Specification<StoredMessage> spec = (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            if (tenant != null && !tenant.isBlank()) {
                predicates.add(cb.equal(root.get("tenant"), tenant));
            }
            if (systemNo != null && !systemNo.isBlank()) {
                predicates.add(cb.equal(root.get("systemNo"), systemNo));
            }
            if (busId != null && !busId.isBlank()) {
                predicates.add(cb.equal(root.get("busId"), busId));
            }
            if (taskId != null && !taskId.isBlank()) {
                predicates.add(cb.equal(root.get("taskId"), taskId));
            }
            if (from != null) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("createdAt"), from));
            }
            if (to != null) {
                predicates.add(cb.lessThanOrEqualTo(root.get("createdAt"), to));
            }
            if (q != null && !q.isBlank()) {
                String like = "%" + q.trim() + "%";
                predicates.add(cb.or(
                        cb.like(root.get("rawJson"), like),
                        cb.like(root.get("adviseKey"), like),
                        cb.like(root.get("nodeName"), like),
                        cb.like(root.get("taskId"), like)
                ));
            }
            return cb.and(predicates.toArray(new Predicate[0]));
        };
        return repository.findAll(spec, PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt")));
    }
}
