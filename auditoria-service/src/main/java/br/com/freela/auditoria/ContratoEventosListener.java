package br.com.freela.auditoria;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class ContratoEventosListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosListener.class);
    private final AuditoriaService service;
    private final JsonMapper jsonMapper;

    public ContratoEventosListener(AuditoriaService service, JsonMapper jsonMapper) {
        this.service = service;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = "${freela.kafka.topico-contrato-eventos}")
    public void onMessage(ConsumerRecord<String, String> record) {
        var evento = jsonMapper.readValue(record.value(), EventoContrato.class);
        log.info("kafka.mensagem.recebida topic={} partition={} offset={} key={} eventId={} eventType={} contratoId={} correlationId={}",
                record.topic(), record.partition(), record.offset(), record.key(),
                evento.eventId(), evento.eventType(), evento.contratoId(), evento.correlationId());
        try {
            var resultado = service.registrar(evento, record.topic(), record.partition(), record.offset(), record.value());
            log.info("kafka.mensagem.processada eventId={} eventType={} contratoId={} resultado={}",
                    evento.eventId(), evento.eventType(), evento.contratoId(), resultado);
        } catch (DataIntegrityViolationException e) {
            log.warn("kafka.mensagem.duplicada eventId={} eventType={} contratoId={} resultado=DUPLICADO_IGNORADO motivo=violacao_unicidade",
                    evento.eventId(), evento.eventType(), evento.contratoId());
        }
    }
}
