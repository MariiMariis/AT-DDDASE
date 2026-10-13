package br.com.freela.auditoria;

import java.time.Instant;
import java.util.UUID;

public record EventoAuditoriaResponse(UUID id, UUID eventId, String eventType, UUID contratoId, String correlationId,
                                      String origem, String topico, Integer particao, Long offset, Instant ocorridoEm,
                                      Instant recebidoEm, String payload) {
    static EventoAuditoriaResponse from(EventoAuditoria e) {
        return new EventoAuditoriaResponse(e.id, e.eventId, e.eventType, e.aggregateId, e.correlationId, e.origem,
                e.topico, e.particao, e.offsetKafka, e.ocorridoEm, e.recebidoEm, e.payload);
    }
}
