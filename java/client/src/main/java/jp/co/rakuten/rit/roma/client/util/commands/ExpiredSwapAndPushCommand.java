package jp.co.rakuten.rit.roma.client.util.commands;

import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.commands.BadCommandException;

public class ExpiredSwapAndPushCommand extends UpdateCommand {

    @Override
    protected void create(CommandContext context) throws BadCommandException {
        // alist_expired_swap_and_push <key> <expire-time> <bytes>\r\n
        // <value>\r\n
        StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
        sb.append(ListCommandID.STR_ALIST_EXPIRED_SWAP_AND_PUSH)
                .append(ListCommandID.STR_WHITE_SPACE)
                .append(context.get(CommandContext.KEY))
                .append(ListCommandID.STR_WHITE_SPACE)
                .append(context.get(UpdateCommand.EXPIRY))
                .append(ListCommandID.STR_WHITE_SPACE)
                .append(((byte[]) context.get(CommandContext.VALUE)).length)
                .append(ListCommandID.STR_CRLF);
        context.put(CommandContext.STRING_DATA, sb);
    }
}
