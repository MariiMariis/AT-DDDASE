package br.com.freela.contrato.infrastructure.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicosConfig {

    @Bean
    NewTopic contratoEventosTopic(@Value("${freela.kafka.topico-contrato-eventos}") String topico,
                                  @Value("${freela.kafka.particoes-contrato-eventos}") int particoes) {
        return TopicBuilder.name(topico).partitions(particoes).replicas(1).build();
    }
}
