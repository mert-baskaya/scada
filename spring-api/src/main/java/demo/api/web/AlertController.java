package demo.api.web;

import demo.api.domain.AlertEntity;
import demo.api.domain.AlertRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class AlertController {

    private final AlertRepository repo;

    public AlertController(AlertRepository repo) {
        this.repo = repo;
    }

    @GetMapping("/alerts")
    public List<AlertEntity> getAlerts(
            @RequestParam(required = false) String componentId,
            @RequestParam(defaultValue = "100") int limit) {
        if (componentId != null && !componentId.isBlank()) {
            return repo.findByComponentIdOrderByTimestampDesc(componentId, PageRequest.of(0, limit));
        }
        return repo.findAll(PageRequest.of(0, limit, Sort.by("timestamp").descending())).getContent();
    }
}
