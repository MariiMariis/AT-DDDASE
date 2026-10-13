package br.com.freela.auditoria;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record EventoContrato(UUID eventId, String eventType, int eventVersion, Instant occurredAt,
                             UUID contratoId, String correlationId, String source) {
    public void validar() {
        if (eventId == null || eventType == null || eventType.isBlank() || contratoId == null || occurredAt == null) {
            throw new EventoInvalidoException("Evento sem campos obrigatórios (eventId, eventType, contratoId, occurredAt)");
        }
    }
}
