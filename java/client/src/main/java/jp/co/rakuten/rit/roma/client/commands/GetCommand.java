package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;
import java.util.StringTokenizer;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;

/**
 * 
 */
public class GetCommand extends DefaultCommand implements CommandID {

    @Override
    public boolean execute(CommandContext context) throws CommandException {
	try {
	    // "get key\r\n"
	    StringBuilder sb = new StringBuilder();
	    sb.append(STR_GET).append(STR_WHITE_SPACE).append(
		    context.get(CommandContext.KEY)).append(STR_CRLR);

	    Connection conn = (Connection) context
		    .get(CommandContext.CONNECTION);
	    conn.out.write(sb.toString().getBytes());
	    conn.out.flush();

	    // VALUE foo 0 <valueLen>\r\n<value>\r\n or END\r\n
	    String s;
	    s = conn.in.readLine();
	    if (s.startsWith("VALUE")) {
		;
	    } else if (s.startsWith("END")) {
		return false;
	    } else if (s.startsWith("ERROR")) {
		//throw new UnsupportedOperationException("Not supported yet.");
		throw new CommandException("Not supported yet.");
	    } else if (s.startsWith("SERVER_ERROR")) {
		throw new CommandException(s);
	    } else if (s.startsWith("CLIENT_ERROR")) {
		throw new CommandException(s);
	    } else {
		//throw new UnsupportedOperationException("Not supported yet.");
		throw new CommandException("Not supported yet.");
	    }

	    StringTokenizer t = new StringTokenizer(s);
	    t.nextToken(); // VALUE
	    t.nextToken(); // key
	    t.nextToken(); // 0

	    // BigInteger valueLen = new BigInteger(t.nextToken()); // len
	    int valueLen = Integer.parseInt(t.nextToken()); // len

	    // value
	    byte[] value = new byte[valueLen];
	    int offset = 0;
	    int size = 0;
	    while (offset < valueLen) {
		size = conn.in.read(value, offset, valueLen - offset);
		offset = offset + size;
	    }
	    context.put(CommandContext.RESULT, value);

	    conn.in.read(2); // "\r\n"
	    conn.in.readLine(); // "END\r\n"
	    return true;
	} catch (IOException e) {
	    throw new CommandException(e);
	}
    }

    @Override
    protected void sendAndReceive(CommandContext context) throws IOException,
	    ClientException {
	throw new UnsupportedOperationException();
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

}
