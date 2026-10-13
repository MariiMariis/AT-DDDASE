package br.com.freela.auditoria;

public class EventoInvalidoException extends RuntimeException {
    public EventoInvalidoException(String mensagem) { super(mensagem); }
    public EventoInvalidoException(String mensagem, Throwable causa) { super(mensagem, causa); }
}
