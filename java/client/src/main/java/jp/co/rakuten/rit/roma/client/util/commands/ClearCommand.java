package jp.co.rakuten.rit.roma.client.util.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;
import jp.co.rakuten.rit.roma.client.commands.BadCommandException;
import jp.co.rakuten.rit.roma.client.commands.DefaultCommand;

/**
 * 
 */
public class ClearCommand extends DefaultCommand {
    @Override
    public boolean execute(CommandContext context) throws CommandException {
	try {
	    // alist_clear <key>\r\n
	    StringBuilder sb = new StringBuilder();
	    sb.append(ListCommandID.STR_ALIST_CLEAR).append(
		    ListCommandID.STR_WHITE_SPACE).append(
		    context.get(CommandContext.KEY)).append(
		    ListCommandID.STR_CRLR);

	    Connection conn = (Connection) context
		    .get(CommandContext.CONNECTION);
	    conn.out.write(sb.toString().getBytes());
	    conn.out.flush();

	    String s = conn.in.readLine();
	    // CLEARED | NOT_CLEARED | NOT_FOUND? | SERVER_ERROR
	    if (s.startsWith("CLEARED")) {
		return true;
	    } else if (s.startsWith("NOT_CLEARED")) {
		return false;
	    } else if (s.startsWith("NOT_FOUND")) {
		return false;
		// throw new ClientException("Not found");
	    } else if (s.startsWith("SERVER_ERROR")) {
		throw new CommandException(s);
	    } else {
		//throw new UnsupportedOperationException(s);
		return false;
	    }
	} catch (IOException e) {
	    throw new CommandException(e);
	}
    }

    @Override
    protected void create(CommandContext context) throws BadCommandException {
	throw new UnsupportedOperationException();
    }

    @Override
    protected boolean parseResult(CommandContext context)
	    throws ClientException {
	throw new UnsupportedOperationException();
    }

    @Override
    protected void sendAndReceive(CommandContext context) throws IOException,
	    ClientException {
	throw new UnsupportedOperationException();
    }
}
