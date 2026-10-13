package br.com.freela.auditoria;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimuladorFalhaTest {

    private final EventoContrato evento = new EventoContrato(UUID.randomUUID(), "ContratoCriado", 1, Instant.now(),
            UUID.randomUUID(), "corr", "contrato-service");

    @Test
    void desligadoPorPadrao() {
        assertThatCode(() -> new SimuladorFalha("").verificar(evento, "{\"titulo\":\"FALHA-SIMULADA\"}")).doesNotThrowAnyException();
    }

    @Test
    void falhaQuandoMensagemContemGatilho() {
        var simulador = new SimuladorFalha("FALHA-SIMULADA");
        assertThatThrownBy(() -> simulador.verificar(evento, "{\"titulo\":\"FALHA-SIMULADA app\"}")).isInstanceOf(FalhaSimuladaException.class);
        assertThatCode(() -> simulador.verificar(evento, "{\"titulo\":\"normal\"}")).doesNotThrowAnyException();
    }
}
