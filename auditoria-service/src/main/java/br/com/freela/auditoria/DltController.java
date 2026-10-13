package br.com.freela.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/auditoria/dlt")
public class DltController {
    private static final Logger log = LoggerFactory.getLogger(DltController.class);
    private final DltReprocessador reprocessador;

    public DltController(DltReprocessador reprocessador) { this.reprocessador = reprocessador; }

    @GetMapping
    public List<DltMensagem> listar() {
        log.info("http.dlt.listar");
        return reprocessador.listar();
    }

    @PostMapping("/reprocessar")
    public ResultadoReprocessamento reprocessar() {
        log.info("http.dlt.reprocessar");
        return reprocessador.reprocessar();
    }
}
