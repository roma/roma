package jp.co.rakuten.rit.roma.client.commands;

import jp.co.rakuten.rit.roma.client.ClientException;

/**
 * 
 */
public class BadCommandException extends ClientException {

    private static final long serialVersionUID = -8399538669461642337L;

    public BadCommandException() {
        super();
    }

    public BadCommandException(Throwable t) {
        super(t);
    }
}
