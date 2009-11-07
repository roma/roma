package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.command.CommandContext;

/**
 * 
 */
public class DeleteCommand extends DefaultCommand implements CommandID {

    @Override
    protected void create(CommandContext context) throws BadCommandException {
        // delete <key> [<time>] [noreply]\r\n
        StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
        sb.append(STR_DELETE).append(STR_WHITE_SPACE).append(
                context.get(CommandContext.KEY)).append(STR_CRLR);
        context.put(CommandContext.STRING_DATA, sb);
    }

    @Override
    protected void sendAndReceive(CommandContext context) throws IOException,
            ClientException {
        StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
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
        StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
        String s = sb.toString();
        if (s.startsWith("DELETED")) {
            return true;
        } else if (s.startsWith("NOT_FOUND")) {
            return false;
        } else if (s.startsWith("SERVER_ERROR")) {
            throw new ClientException(s);
        } else if (s.startsWith("CLIENT_ERROR")) {
            throw new ClientException(s);
        } else {
            //throw new UnsupportedOperationException();
            throw new ClientException("not support yet");
        }
    }
}
