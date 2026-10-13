package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContratoTest {

    private Contrato novoContrato() {
        return Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "API de pagamentos", new BigDecimal("3500.00"));
    }

    @Test
    void criarEmiteContratoCriado() {
        var contrato = novoContrato();
        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
        assertThat(contrato.pullDomainEvents()).singleElement().isInstanceOf(ContratoCriado.class);
        assertThat(contrato.domainEvents()).isEmpty();
    }

    @Test
    void cicloCompletoEmiteEventosEmOrdem() {
        var contrato = novoContrato();
        contrato.registrarEntrega();
        contrato.concluir();
        var eventos = contrato.pullDomainEvents();
        assertThat(eventos).extracting(DomainEvent::eventType)
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
        assertThat(eventos).extracting(DomainEvent::aggregateId).containsOnly(contrato.id());
        assertThat(eventos.get(1)).isInstanceOf(EntregaRegistrada.class);
        assertThat(((ContratoConcluido) eventos.get(2)).status()).isEqualTo(StatusContrato.CONCLUIDO);
    }

    @Test
    void cancelarEmiteContratoCancelado() {
        var contrato = novoContrato();
        contrato.pullDomainEvents();
        contrato.cancelar();
        assertThat(contrato.pullDomainEvents()).singleElement().isInstanceOf(ContratoCancelado.class);
    }

    @Test
    void transicaoInvalidaNaoEmiteEvento() {
        var contrato = novoContrato();
        contrato.pullDomainEvents();
        assertThatThrownBy(contrato::concluir).isInstanceOf(IllegalStateException.class);
        assertThat(contrato.domainEvents()).isEmpty();
    }

    @Test
    void naoPermiteCancelarDuasVezes() {
        var contrato = novoContrato();
        contrato.cancelar();
        assertThatThrownBy(contrato::cancelar).isInstanceOf(IllegalStateException.class);
    }
}
