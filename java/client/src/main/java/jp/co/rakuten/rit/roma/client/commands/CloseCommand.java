package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.command.CommandContext;

/**
 * 
 */
public class CloseCommand extends DefaultCommand {

    @Override
    protected void create(CommandContext context) throws BadCommandException {
	// TODO Auto-generated method stub
    }

    @Override
    protected boolean parseResult(CommandContext context)
	    throws ClientException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    protected void sendAndReceive(CommandContext context) throws IOException,
	    ClientException {
	// TODO Auto-generated method stub

    }

}
