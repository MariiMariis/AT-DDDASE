package br.com.freela.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
public class RequestLoggingFilter implements GlobalFilter, Ordered {
    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst("X-Correlation-Id");
        if (correlationId == null || correlationId.isBlank()) correlationId = UUID.randomUUID().toString();
        String finalCorrelationId = correlationId;
        long inicio = System.currentTimeMillis();

        var request = exchange.getRequest().mutate().header("X-Correlation-Id", correlationId).build();
        try (var ignorado = MDC.putCloseable("correlationId", correlationId)) {
            log.info("gateway.request.inicio correlationId={} method={} path={}", correlationId,
                    request.getMethod(), request.getURI().getPath());
        }

        return chain.filter(exchange.mutate().request(request).build())
                .doFinally(signal -> {
                    try (var ignorado = MDC.putCloseable("correlationId", finalCorrelationId)) {
                        log.info("gateway.request.fim correlationId={} method={} path={} status={} durationMs={} signal={}",
                                finalCorrelationId, request.getMethod(), request.getURI().getPath(),
                                exchange.getResponse().getStatusCode(), System.currentTimeMillis() - inicio, signal);
                    }
                });
    }

    @Override public int getOrder() { return -100; }
}
