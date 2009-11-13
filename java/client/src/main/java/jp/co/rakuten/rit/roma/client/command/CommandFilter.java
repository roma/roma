package jp.co.rakuten.rit.roma.client.command;

/**
 * 
 */
public interface CommandFilter extends Command {

    void preExecute(CommandContext context) throws CommandException;

    void postExecute(CommandContext context) throws CommandException;

    boolean aroundExecute(Command command, CommandContext context)
            throws CommandException;
}
