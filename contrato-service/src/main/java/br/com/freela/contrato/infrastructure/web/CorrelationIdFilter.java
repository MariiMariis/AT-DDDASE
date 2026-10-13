package br.com.freela.contrato.infrastructure.web;

import io.micrometer.tracing.Tracer;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class CorrelationIdFilter extends OncePerRequestFilter {
    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";
    public static final String MDC_CONTRATO_ID = "contratoId";
    private final Tracer tracer;

    public CorrelationIdFilter(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = request.getHeader(HEADER);
        if (correlationId == null || correlationId.isBlank()) correlationId = UUID.randomUUID().toString();
        MDC.put(MDC_KEY, correlationId);
        response.setHeader(HEADER, correlationId);
        var span = tracer.currentSpan();
        if (span != null) span.tag(MDC_KEY, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_KEY);
            MDC.remove(MDC_CONTRATO_ID);
        }
    }
}
