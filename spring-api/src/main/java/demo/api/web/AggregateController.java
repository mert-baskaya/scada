package demo.api.web;

import demo.api.domain.AggregateEntity;
import demo.api.domain.AggregateRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class AggregateController {

    private final AggregateRepository repo;

    public AggregateController(AggregateRepository repo) {
        this.repo = repo;
    }

    @GetMapping("/aggregates")
    public List<AggregateEntity> getAggregates(
            @RequestParam(required = false) String componentId,
            @RequestParam(defaultValue = "100") int limit) {
        if (componentId != null && !componentId.isBlank()) {
            return repo.findByComponentIdOrderByWindowStartDesc(componentId, PageRequest.of(0, limit));
        }
        return repo.findAll(PageRequest.of(0, limit, Sort.by("windowStart").descending())).getContent();
    }

    @GetMapping("/components")
    public List<String> getComponents() {
        return repo.findDistinctComponentIds();
    }
}
