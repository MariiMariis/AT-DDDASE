package br.com.freela.notificacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SimuladorFalha {
    private static final Logger log = LoggerFactory.getLogger(SimuladorFalha.class);
    private final String gatilho;

    public SimuladorFalha(@Value("${freela.simulacao.falha-titulo:}") String gatilho) {
        this.gatilho = gatilho == null ? "" : gatilho.trim();
        if (!this.gatilho.isEmpty()) log.warn("simulacao.falha.ativa gatilho={}", this.gatilho);
    }

    public void verificar(EventoContrato evento, String mensagem) {
        if (!gatilho.isEmpty() && mensagem.contains(gatilho)) {
            throw new FalhaSimuladaException("Falha simulada no processamento do evento " + evento.eventId());
        }
    }
}
