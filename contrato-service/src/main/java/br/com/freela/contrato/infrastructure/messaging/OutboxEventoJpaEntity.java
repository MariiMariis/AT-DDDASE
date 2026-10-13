package br.com.freela.contrato.infrastructure.messaging;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_eventos", indexes = @Index(name = "idx_outbox_aggregate", columnList = "aggregateid"))
public class OutboxEventoJpaEntity {
    @Id private UUID id;
    @Column(name = "aggregatetype", nullable = false, length = 60) private String aggregateType;
    @Column(name = "aggregateid", nullable = false, length = 36) private String aggregateId;
    @Column(name = "type", nullable = false, length = 60) private String type;
    @Column(name = "payload", nullable = false, columnDefinition = "text") private String payload;
    @Column(name = "correlation_id", length = 100) private String correlationId;
    @Column(name = "traceparent", length = 100) private String traceparent;
    @Column(name = "occurred_at", nullable = false) private Instant occurredAt;
    protected OutboxEventoJpaEntity() {}
    public OutboxEventoJpaEntity(UUID id, String aggregateType, String aggregateId, String type, String payload,
                                 String correlationId, String traceparent, Instant occurredAt) {
        this.id = id; this.aggregateType = aggregateType; this.aggregateId = aggregateId; this.type = type;
        this.payload = payload; this.correlationId = correlationId; this.traceparent = traceparent; this.occurredAt = occurredAt;
    }
    public UUID getId() { return id; } public String getAggregateType() { return aggregateType; }
    public String getAggregateId() { return aggregateId; } public String getType() { return type; }
    public String getPayload() { return payload; } public String getCorrelationId() { return correlationId; }
    public String getTraceparent() { return traceparent; }
    public Instant getOccurredAt() { return occurredAt; }
}
