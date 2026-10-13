package br.com.freela.notificacao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "eventos_processados")
class EventoProcessado implements Persistable<UUID> {
    @Id private UUID eventId;
    @Column(nullable = false, length = 60) private String eventType;
    @Column(nullable = false) private UUID contratoId;
    @Column(nullable = false) private Instant processadoEm;
    @Transient private boolean novo = true;

    protected EventoProcessado() { this.novo = false; }

    EventoProcessado(EventoContrato evento) {
        this.eventId = evento.eventId();
        this.eventType = evento.eventType();
        this.contratoId = evento.contratoId();
        this.processadoEm = Instant.now();
    }

    @Override public UUID getId() { return eventId; }
    @Override public boolean isNew() { return novo; }
}
