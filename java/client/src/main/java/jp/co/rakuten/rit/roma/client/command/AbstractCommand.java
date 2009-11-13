package jp.co.rakuten.rit.roma.client.command;

/**
 * 
 */
public abstract class AbstractCommand implements Command {

    public Command addFilter(CommandFilter filter) {
        if (filter == null) {
            throw new IllegalArgumentException();
        }

        AbstractCommandFilter f = (AbstractCommandFilter) filter;
        f.setCommand(this);
        return f;
    }

    public abstract boolean execute(CommandContext context)
            throws CommandException;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Command: ").append(getClass().getName());
        return sb.toString();
    }
}
