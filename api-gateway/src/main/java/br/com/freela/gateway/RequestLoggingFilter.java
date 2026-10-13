package br.com.freela.gateway;

import io.micrometer.common.KeyValue;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.handler.TracingObservationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.observation.ServerRequestObservationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
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
        var observacao = ServerRequestObservationContext.findCurrent(exchange.getAttributes());
        observacao.ifPresent(contexto -> contexto.addHighCardinalityKeyValue(KeyValue.of("correlationId", finalCorrelationId)));
        Map<String, String> contextoLog = new HashMap<>();
        contextoLog.put("correlationId", correlationId);
        observacao.<TracingObservationHandler.TracingContext>map(contexto -> contexto.get(TracingObservationHandler.TracingContext.class))
                .map(TracingObservationHandler.TracingContext::getSpan)
                .ifPresent(span -> {
                    TraceContext trace = span.context();
                    contextoLog.put("traceId", trace.traceId());
                    contextoLog.put("spanId", trace.spanId());
                });

        comContexto(contextoLog, () -> log.info("gateway.request.inicio correlationId={} method={} path={}",
                finalCorrelationId, request.getMethod(), request.getURI().getPath()));

        return chain.filter(exchange.mutate().request(request).build())
                .doFinally(signal -> comContexto(contextoLog, () -> log.info(
                        "gateway.request.fim correlationId={} method={} path={} status={} durationMs={} signal={}",
                        finalCorrelationId, request.getMethod(), request.getURI().getPath(),
                        exchange.getResponse().getStatusCode(), System.currentTimeMillis() - inicio, signal)));
    }

    private static void comContexto(Map<String, String> contexto, Runnable acao) {
        var anterior = MDC.getCopyOfContextMap();
        try {
            contexto.forEach(MDC::put);
            acao.run();
        } finally {
            if (anterior == null) MDC.clear(); else MDC.setContextMap(anterior);
        }
    }

    @Override public int getOrder() { return -100; }
}
