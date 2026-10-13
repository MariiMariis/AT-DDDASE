package br.com.freela.contrato.application;

import br.com.freela.contrato.application.port.EventoOutbox;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final EventoOutbox outbox;

    public ContratoApplicationService(ContratoRepository repository, EventoOutbox outbox) {
        this.repository = repository;
        this.outbox = outbox;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={}", cmd.clienteId(), cmd.freelancerId());
        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());
        Contrato salvo = repository.salvar(contrato);
        registrarEventos(contrato);
        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        return transicionar(id, "entrega", Contrato::registrarEntrega);
    }

    @Transactional
    public Contrato concluir(UUID id) {
        return transicionar(id, "conclusao", Contrato::concluir);
    }

    @Transactional
    public Contrato cancelar(UUID id) {
        return transicionar(id, "cancelamento", Contrato::cancelar);
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new ContratoNaoEncontradoException(id));
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }

    private Contrato transicionar(UUID id, String operacao, Consumer<Contrato> transicao) {
        log.info("contrato.{}.inicio contratoId={}", operacao, id);
        var contrato = repository.buscarParaAtualizacao(id).orElseThrow(() -> new ContratoNaoEncontradoException(id));
        var statusAnterior = contrato.status();
        transicao.accept(contrato);
        var salvo = repository.salvar(contrato);
        registrarEventos(contrato);
        log.info("contrato.{}.sucesso contratoId={} statusAnterior={} status={}", operacao, id, statusAnterior, salvo.status());
        return salvo;
    }

    private void registrarEventos(Contrato contrato) {
        List<DomainEvent> eventos = contrato.pullDomainEvents();
        outbox.registrar(eventos);
        eventos.forEach(e -> log.info("contrato.evento.outbox contratoId={} eventId={} eventType={} occurredAt={}",
                e.aggregateId(), e.eventId(), e.eventType(), e.occurredAt()));
    }
}
