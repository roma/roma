package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.command.AbstractCommand;
import jp.co.rakuten.rit.roma.client.command.Command;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;

/**
 * 
 */
public abstract class DefaultCommand extends AbstractCommand implements
        Command, CommandID {

    @Override
    public boolean execute(CommandContext context) throws CommandException {
        try {
            StringBuilder sb = new StringBuilder();
            context.put(CommandContext.STRING_DATA, sb);
            create(context);
            sendAndReceive(context);
            return parseResult(context);
        } catch (ClientException e) {
            throw new CommandException(e);
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    protected abstract void create(CommandContext context)
            throws BadCommandException;

    protected abstract void sendAndReceive(CommandContext context)
            throws IOException, ClientException;

    protected abstract boolean parseResult(CommandContext context)
            throws ClientException;
}
