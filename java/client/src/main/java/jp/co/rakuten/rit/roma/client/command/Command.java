package jp.co.rakuten.rit.roma.client.command;

/**
 * 
 */
public interface Command {
    boolean execute(CommandContext context) throws CommandException;

    Command addFilter(CommandFilter filter);
}
