package jp.co.rakuten.rit.roma.client.commands;

import jp.co.rakuten.rit.roma.client.command.CommandException;

/**
 * 
 */
public class RetryOutException extends CommandException {

    public RetryOutException() {
        super("Retry out");
    }

    public RetryOutException(Throwable t) {
        super(t);
    }
    private static final long serialVersionUID = 2963394216686818649L;
}
