package jp.co.rakuten.rit.roma.client.command;

/**
 * 
 */
public class CommandChain extends AbstractCommand implements Command {
    protected AbstractCommand[] commands = new AbstractCommand[0];

    public void addCommand(Command command) {
	if (command == null) {
	    throw new NullPointerException();
	}

	AbstractCommand[] srcCommands = commands;
	int destLen = srcCommands.length + 1;
	AbstractCommand[] destCommands = new AbstractCommand[destLen];
	int index = 0;
	for (; index < srcCommands.length; ++index) {
	    destCommands[index] = srcCommands[index];
	}
	destCommands[index] = (AbstractCommand) command;
	commands = destCommands;
    }

    @Override
    public boolean execute(CommandContext context) throws CommandException {
	if (context == null) {
	    throw new IllegalArgumentException();
	}

	boolean ret = true;
	for (int i = 0; i < commands.length; ++i) {
	    ret = commands[i].execute(context);
	    if (!ret) {
		return ret;
	    }
	}
	return true;
    }

}
