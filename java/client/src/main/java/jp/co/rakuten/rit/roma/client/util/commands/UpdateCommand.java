package jp.co.rakuten.rit.roma.client.util.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.commands.BadCommandException;
import jp.co.rakuten.rit.roma.client.commands.DefaultCommand;

/**
 * 
 */
public class UpdateCommand extends DefaultCommand {
    public static final String INDEX = "index";

    public static final String ARRAY_SIZE = "array-size";

    public static final String SEP = "_$$_";

    public static final String EXPIRY = "expiry";

    @Override
    protected void create(CommandContext context) throws BadCommandException {
	throw new UnsupportedOperationException();
    }

    @Override
    protected void sendAndReceive(CommandContext context) throws IOException,
	    ClientException {
	StringBuilder sb = (StringBuilder) context
		.get(CommandContext.STRING_DATA);
	Connection conn = (Connection) context.get(CommandContext.CONNECTION);
	conn.out.write(sb.toString().getBytes());
	conn.out.write((byte[]) context.get(CommandContext.VALUE));
	conn.out.write(ListCommandID.STR_CRLR.getBytes());
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
	String s = sb.toString();
	// STORED | NOT_STORED | SERVER_ERROR
	if (s.startsWith("STORED")) {
	    return true;
	} else if (s.startsWith("NOT_STORED")) {
	    return false;
	} else if (s.startsWith("SERVER_ERROR")) {
	    throw new ClientException(s);
	} else {
	    // throw new UnsupportedOperationException();
	    return false;
	}
    }
}
