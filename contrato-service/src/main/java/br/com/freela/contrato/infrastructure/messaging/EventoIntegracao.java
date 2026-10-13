package br.com.freela.contrato.infrastructure.messaging;

import java.time.Instant;
import java.util.UUID;

public record EventoIntegracao(UUID eventId, String eventType, int eventVersion, Instant occurredAt,
                               UUID contratoId, String correlationId, String source, ContratoPayload payload) {
    public static final int VERSAO = 1;
    public static final String SOURCE = "contrato-service";
}
