package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;
import java.math.BigInteger;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.command.CommandContext;

/**
 * 
 */
public class IncrAndDecrCommand extends DefaultCommand {

    @Override
    protected void create(CommandContext context) throws BadCommandException {
	StringBuilder sb = (StringBuilder) context
		.get(CommandContext.STRING_DATA);
	sb.append(getCommand()).append(STR_WHITE_SPACE).append(
		context.get(CommandContext.KEY)).append(STR_WHITE_SPACE)
		.append(context.get(CommandContext.VALUE)).append(STR_CRLR);
	context.put(CommandContext.STRING_DATA, sb);
    }

    protected String getCommand() throws BadCommandException {
	throw new BadCommandException();
    }

    @Override
    protected void sendAndReceive(CommandContext context) throws IOException,
	    ClientException {
	StringBuilder sb = (StringBuilder) context
		.get(CommandContext.STRING_DATA);
	Connection conn = (Connection) context.get(CommandContext.CONNECTION);
	conn.out.write(sb.toString().getBytes());
	conn.out.flush();
	sb = new StringBuilder();
	sb.append(conn.in.readLine());
	context.put(CommandContext.STRING_DATA, sb);
    }

    @Override
    protected boolean parseResult(CommandContext context)
	    throws ClientException {
	StringBuilder sb = (StringBuilder) context
		.get(CommandContext.STRING_DATA);
	String ret = sb.toString();
	if (ret.startsWith("NOT_FOUND")) {
	    return false;
	    //throw new ClientException(
	    /*
	    throw new UnsupportedOperationException(
		    "Not supported yet: NOT_FOUND");
		    */
	} else { // big integer
	    context.put(CommandContext.RESULT, new BigInteger(ret));
	    return true;
	}
    }

}
