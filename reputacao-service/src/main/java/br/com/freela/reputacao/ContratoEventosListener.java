package br.com.freela.reputacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class ContratoEventosListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosListener.class);
    private final ReputacaoService service;
    private final JsonMapper jsonMapper;

    public ContratoEventosListener(ReputacaoService service, JsonMapper jsonMapper) {
        this.service = service;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = "${freela.kafka.topico-contrato-eventos}")
    public void onMessage(ConsumerRecord<String, String> record) {
        var evento = jsonMapper.readValue(record.value(), EventoContrato.class);
        log.info("kafka.mensagem.recebida topic={} partition={} offset={} key={} eventId={} eventType={} contratoId={} correlationId={}",
                record.topic(), record.partition(), record.offset(), record.key(),
                evento.eventId(), evento.eventType(), evento.contratoId(), evento.correlationId());
        boolean alterou = service.processar(evento);
        log.info("kafka.mensagem.processada eventId={} eventType={} contratoId={} reputacaoAlterada={}",
                evento.eventId(), evento.eventType(), evento.contratoId(), alterou);
    }
}
