package jp.co.rakuten.rit.roma.client.util.commands;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.commands.BadCommandException;

/**
 * 
 */
public class SwapAndSizedPushCommand extends UpdateCommand {

    @Override
    protected void create(CommandContext context) throws BadCommandException {
        // alist_swap_and_sized_push <key> <array-size> <bytes>\r\n
        // <value>\r\n
        StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
        sb.append(ListCommandID.STR_ALIST_SWAP_AND_SIZED_PUSH).append(
                ListCommandID.STR_WHITE_SPACE).append(
                context.get(CommandContext.KEY)).append(
                ListCommandID.STR_WHITE_SPACE).append(
                context.get(UpdateCommand.ARRAY_SIZE)) // array-size
                .append(ListCommandID.STR_WHITE_SPACE).append(
                ((byte[]) context.get(CommandContext.VALUE)).length).append(ListCommandID.STR_CRLF);
        context.put(CommandContext.STRING_DATA, sb);
    }

    @Override
    protected boolean parseResult(CommandContext context)
            throws ClientException {
        StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
        String s = sb.toString();
        // STORED | NOT_STORED | SERVER_ERROR
        if (s.startsWith("STORED")) {
            return true;
        } else if (s.startsWith("NOT_STORED")) {
            return false;
        } else if (s.startsWith("NOT_PUSHED")) {
            return false;
        } else if (s.startsWith("SERVER_ERROR")) {
            throw new ClientException(s);
        } else {
            //throw new UnsupportedOperationException();
            return false;
        }
    }
}
