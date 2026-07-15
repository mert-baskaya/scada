package demo.api.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import demo.api.domain.AggregateEntity;
import demo.api.domain.AggregateRepository;
import demo.api.domain.AlertEntity;
import demo.api.domain.AlertRepository;
import demo.api.sse.SseBroadcaster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class ResultsListener {

    private final ObjectMapper objectMapper;
    private final AggregateRepository aggregateRepo;
    private final AlertRepository alertRepo;
    private final SseBroadcaster broadcaster;

    public ResultsListener(ObjectMapper objectMapper, AggregateRepository aggregateRepo,
                           AlertRepository alertRepo, SseBroadcaster broadcaster) {
        this.objectMapper = objectMapper;
        this.aggregateRepo = aggregateRepo;
        this.alertRepo = alertRepo;
        this.broadcaster = broadcaster;
    }

    @KafkaListener(topics = "scada.aggregates", groupId = "${spring.kafka.consumer.group-id}")
    public void onAggregate(ConsumerRecord<String, String> record) throws Exception {
        JsonNode json = objectMapper.readTree(record.value());
        AggregateEntity entity = new AggregateEntity();
        entity.setComponentId(json.get("componentId").asText());
        entity.setReadingCount(json.get("readingCount").asInt());
        entity.setAvgVoltage(json.get("avgVoltage").asDouble());
        entity.setMinVoltage(json.get("minVoltage").asDouble());
        entity.setMaxVoltage(json.get("maxVoltage").asDouble());
        entity.setAvgCurrent(json.get("avgCurrent").asDouble());
        if (json.has("maxOilTemp") && !json.get("maxOilTemp").isNull()) {
            entity.setMaxOilTemp(json.get("maxOilTemp").asDouble());
        }
        if (json.has("windowStart")) {
            entity.setWindowStart(Instant.parse(json.get("windowStart").asText()));
        }
        if (json.has("windowEnd")) {
            entity.setWindowEnd(Instant.parse(json.get("windowEnd").asText()));
        }
        entity.setReceivedAt(Instant.now());
        aggregateRepo.save(entity);
        broadcaster.broadcast("aggregate", json);
    }

    @KafkaListener(topics = "scada.alerts", groupId = "${spring.kafka.consumer.group-id}")
    public void onAlert(ConsumerRecord<String, String> record) throws Exception {
        JsonNode json = objectMapper.readTree(record.value());
        AlertEntity entity = new AlertEntity();
        entity.setComponentId(json.get("componentId").asText());
        if (json.has("substationId") && !json.get("substationId").isNull()) {
            entity.setSubstationId(json.get("substationId").asText());
        }
        entity.setAlertType(json.get("alertType").asText());
        entity.setMessage(json.get("message").asText());
        if (json.has("timestamp")) {
            entity.setTimestamp(Instant.parse(json.get("timestamp").asText()));
        }
        entity.setReceivedAt(Instant.now());
        alertRepo.save(entity);
        broadcaster.broadcast("alert", json);
    }
}
