package br.com.freela.reputacao;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlingConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    NewTopic contratoEventosDltTopic(@Value("${freela.kafka.topico-dlt}") String topico,
                                     @Value("${freela.kafka.particoes-dlt}") int particoes) {
        return TopicBuilder.name(topico).partitions(particoes).replicas(1).build();
    }

    @Bean
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<?, ?> template,
                                                                @Value("${freela.kafka.topico-dlt}") String topicoDlt) {
        return new DeadLetterPublishingRecoverer(template, (record, ex) -> new TopicPartition(topicoDlt, record.partition()));
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer,
                                          @Value("${freela.kafka.topico-dlt}") String topicoDlt,
                                          @Value("${freela.kafka.retry.tentativas}") int tentativas,
                                          @Value("${freela.kafka.retry.intervalo-inicial-ms}") long intervaloInicial,
                                          @Value("${freela.kafka.retry.multiplicador}") double multiplicador,
                                          @Value("${freela.kafka.retry.intervalo-maximo-ms}") long intervaloMaximo) {
        var backOff = new ExponentialBackOffWithMaxRetries(tentativas);
        backOff.setInitialInterval(intervaloInicial);
        backOff.setMultiplier(multiplicador);
        backOff.setMaxInterval(intervaloMaximo);

        var handler = new DefaultErrorHandler((record, ex) -> {
            var causa = causa(ex);
            log.error("kafka.mensagem.dlt topic={} partition={} offset={} key={} topicoDlt={} excecao={} motivo={}",
                    record.topic(), record.partition(), record.offset(), record.key(), topicoDlt,
                    causa.getClass().getSimpleName(), causa.getMessage());
            recoverer.accept(record, causa instanceof Exception e ? e : ex);
        }, backOff);
        handler.addNotRetryableExceptions(EventoInvalidoException.class);
        handler.setRetryListeners((record, ex, tentativa) -> {
            var causa = causa(ex);
            log.warn("kafka.mensagem.retentativa topic={} partition={} offset={} key={} tentativa={} excecao={} motivo={}",
                    record.topic(), record.partition(), record.offset(), record.key(), tentativa,
                    causa.getClass().getSimpleName(), causa.getMessage());
        });
        return handler;
    }

    private static Throwable causa(Exception ex) {
        return ex instanceof ListenerExecutionFailedException && ex.getCause() != null ? ex.getCause() : ex;
    }
}
