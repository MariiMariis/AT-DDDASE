package br.com.freela.reputacao;

public class EventoInvalidoException extends RuntimeException {
    public EventoInvalidoException(String mensagem) { super(mensagem); }
    public EventoInvalidoException(String mensagem, Throwable causa) { super(mensagem, causa); }
}
