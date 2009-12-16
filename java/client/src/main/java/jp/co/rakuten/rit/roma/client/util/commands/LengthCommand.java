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
public class LengthCommand extends DefaultCommand {

    @Override
    public boolean execute(CommandContext context) throws CommandException {
        try {
            // alist_length <key>\r\n
            StringBuilder sb = new StringBuilder();
            sb.append(ListCommandID.STR_ALIST_LENGTH).append(
                    ListCommandID.STR_WHITE_SPACE).append(
                    context.get(CommandContext.KEY)).append(
                    ListCommandID.STR_CRLF);

            Connection conn = (Connection) context
                    .get(CommandContext.CONNECTION);
            conn.out.write(sb.toString().getBytes());
            conn.out.flush();

            String s = conn.in.readLine();
            // length | NOT_FOUND | SERVER_ERROR
            if (s.startsWith("SERVER_ERROR")) {
                // return -1;
                throw new CommandException(s);
            } else if (s.startsWith("NOT_FOUND")) {
                context.put(CommandContext.RESULT, new Integer(-1));
                return false;
                // throw new ClientException("Not found");
            } else {
                context.put(CommandContext.RESULT, new Integer(s));
                return true;
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
