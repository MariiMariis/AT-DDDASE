package br.com.freela.contrato.application.port;

import br.com.freela.contrato.domain.shared.DomainEvent;
import java.util.List;

public interface EventoOutbox {
    void registrar(List<DomainEvent> eventos);
}
