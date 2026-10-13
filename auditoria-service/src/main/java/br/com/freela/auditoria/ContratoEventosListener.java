package br.com.freela.auditoria;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
public class ContratoEventosListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosListener.class);
    private final AuditoriaService service;
    private final JsonMapper jsonMapper;
    private final SimuladorFalha simuladorFalha;

    public ContratoEventosListener(AuditoriaService service, JsonMapper jsonMapper, SimuladorFalha simuladorFalha) {
        this.service = service;
        this.jsonMapper = jsonMapper;
        this.simuladorFalha = simuladorFalha;
    }

    @KafkaListener(topics = "${freela.kafka.topico-contrato-eventos}")
    public void onMessage(ConsumerRecord<String, String> record) {
        processar(record);
    }

    public void processar(ConsumerRecord<String, String> record) {
        var evento = ler(record);
        log.info("kafka.mensagem.recebida topic={} partition={} offset={} key={} eventId={} eventType={} contratoId={} correlationId={}",
                record.topic(), record.partition(), record.offset(), record.key(),
                evento.eventId(), evento.eventType(), evento.contratoId(), evento.correlationId());
        simuladorFalha.verificar(evento, record.value());
        try {
            var resultado = service.registrar(evento, record.topic(), record.partition(), record.offset(), record.value());
            log.info("kafka.mensagem.processada eventId={} eventType={} contratoId={} resultado={}",
                    evento.eventId(), evento.eventType(), evento.contratoId(), resultado);
        } catch (DataIntegrityViolationException e) {
            log.warn("kafka.mensagem.duplicada eventId={} eventType={} contratoId={} resultado=DUPLICADO_IGNORADO motivo=violacao_unicidade",
                    evento.eventId(), evento.eventType(), evento.contratoId());
        }
    }

    private EventoContrato ler(ConsumerRecord<String, String> record) {
        try {
            if (record.value() == null || record.value().isBlank()) throw new EventoInvalidoException("Mensagem sem conteúdo");
            var evento = jsonMapper.readValue(record.value(), EventoContrato.class);
            evento.validar();
            return evento;
        } catch (JacksonException e) {
            log.warn("kafka.mensagem.invalida topic={} partition={} offset={} key={} motivo=json_invalido",
                    record.topic(), record.partition(), record.offset(), record.key());
            throw new EventoInvalidoException("Mensagem com JSON inválido", e);
        } catch (EventoInvalidoException e) {
            log.warn("kafka.mensagem.invalida topic={} partition={} offset={} key={} motivo={}",
                    record.topic(), record.partition(), record.offset(), record.key(), e.getMessage());
            throw e;
        }
    }
}
