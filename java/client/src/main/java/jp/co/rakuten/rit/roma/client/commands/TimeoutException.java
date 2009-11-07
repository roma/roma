package jp.co.rakuten.rit.roma.client.commands;

import jp.co.rakuten.rit.roma.client.command.CommandException;

/**
 * 
 */
public class TimeoutException extends CommandException {

    private static final long serialVersionUID = 1262780445524677010L;

    public TimeoutException(Throwable t) {
        super(t);
    }
}
