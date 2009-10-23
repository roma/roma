package jp.co.rakuten.rit.roma.client.command;

/**
 * 
 */
public interface CommandGenerator {

    public Command getCommand(int commandID);

    public void createCommand(int commandID,
	    Class<? extends Command> commandClass)
	    throws InstantiationException, IllegalAccessException;

    public void createCommand(int commandID,
	    Class<? extends Command> commandClass,
	    Class<? extends CommandFilter>[] filterClasses)
	    throws InstantiationException, IllegalAccessException;
}
