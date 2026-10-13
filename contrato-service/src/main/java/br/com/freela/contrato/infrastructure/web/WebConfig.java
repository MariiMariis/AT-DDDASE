package br.com.freela.contrato.infrastructure.web;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {
    private final ContratoIdMdcInterceptor contratoIdMdcInterceptor;

    public WebConfig(ContratoIdMdcInterceptor contratoIdMdcInterceptor) {
        this.contratoIdMdcInterceptor = contratoIdMdcInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(contratoIdMdcInterceptor).addPathPatterns("/api/contratos/**");
    }
}
