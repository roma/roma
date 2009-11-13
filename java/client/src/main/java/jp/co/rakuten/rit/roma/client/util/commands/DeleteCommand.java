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
public class DeleteCommand extends DefaultCommand {

    @Override
    public boolean execute(CommandContext context) throws CommandException {
        try {
            // alist_delete <key> <bytes>\r\n
            // <element>\r\n
            StringBuilder sb = new StringBuilder();
            sb.append(ListCommandID.STR_ALIST_DELETE)
                    .append(ListCommandID.STR_WHITE_SPACE)
                    .append(context.get(CommandContext.KEY))
                    .append(ListCommandID.STR_WHITE_SPACE)
                    .append(((byte[]) context.get(CommandContext.VALUE)).length)
                    .append(ListCommandID.STR_CRLR);

            Connection conn = (Connection) context.get(CommandContext.CONNECTION);
            conn.out.write(sb.toString().getBytes());
            conn.out.write((byte[]) context.get(CommandContext.VALUE));
            conn.out.write(ListCommandID.STR_CRLR.getBytes());
            conn.out.flush();

            String s = conn.in.readLine();
            // DELETED | NOT_DELETED | NOT_FOUND | SERVER_ERROR
            if (s.startsWith("DELETED")) {
                return true;
            } else if (s.startsWith("NOT_DELETED")) {
                return false;
            } else if (s.startsWith("NOT_FOUND")) {
                // throw new ClientException("Not found");
                return false;
            } else if (s.startsWith("SERVER_ERROR")) {
                throw new CommandException(s);
            } else {
                //throw new UnsupportedOperationException();
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
