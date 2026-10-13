package br.com.freela.notificacao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record EventoContrato(UUID eventId, String eventType, int eventVersion, Instant occurredAt,
                             UUID contratoId, String correlationId, String source, Payload payload) {
    public void validar() {
        if (eventId == null || eventType == null || eventType.isBlank() || contratoId == null || occurredAt == null) {
            throw new EventoInvalidoException("Evento sem campos obrigatórios (eventId, eventType, contratoId, occurredAt)");
        }
        if (payload == null || payload.clienteId() == null || payload.freelancerId() == null || payload.valor() == null) {
            throw new EventoInvalidoException("Evento sem payload obrigatório (clienteId, freelancerId, valor)");
        }
    }
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor, String status) {}
}
