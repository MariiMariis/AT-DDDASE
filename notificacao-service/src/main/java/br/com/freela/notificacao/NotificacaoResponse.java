package br.com.freela.notificacao;

import java.time.Instant;
import java.util.UUID;

public record NotificacaoResponse(UUID id, UUID eventId, UUID contratoId, UUID destinatarioId, String papelDestinatario,
                                  String tipo, String mensagem, String correlationId, Instant criadaEm) {
    static NotificacaoResponse from(Notificacao n) {
        return new NotificacaoResponse(n.id, n.eventId, n.contratoId, n.destinatarioId, n.papelDestinatario,
                n.tipo, n.mensagem, n.correlationId, n.criadaEm);
    }
}
