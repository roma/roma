package jp.co.rakuten.rit.roma.client.command;

import jp.co.rakuten.rit.roma.client.ClientException;

/**
 * 
 */
public class CommandException extends ClientException {

    private static final long serialVersionUID = 7002705072814047911L;

    public CommandException(Throwable t) {
	super(t);
    }

    public CommandException(String reason) {
	super(reason);
    }

    public CommandException(String reason, Throwable t) {
	super(reason, t);
    }

}
