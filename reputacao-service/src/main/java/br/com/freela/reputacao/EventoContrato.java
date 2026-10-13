package br.com.freela.reputacao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record EventoContrato(UUID eventId, String eventType, int eventVersion, Instant occurredAt,
                             UUID contratoId, String correlationId, String source, Payload payload) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor, String status) {}
}
