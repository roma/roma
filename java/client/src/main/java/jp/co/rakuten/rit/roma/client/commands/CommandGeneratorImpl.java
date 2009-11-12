package jp.co.rakuten.rit.roma.client.commands;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jp.co.rakuten.rit.roma.client.command.Command;
import jp.co.rakuten.rit.roma.client.command.CommandFilter;
import jp.co.rakuten.rit.roma.client.command.CommandGenerator;

/**
 * 
 */
public class CommandGeneratorImpl implements CommandGenerator {

    //protected HashMap<Integer, Command> commands = new HashMap<Integer, Command>();
    protected Map<Integer, Command> commands = new ConcurrentHashMap<Integer, Command>();

    public CommandGeneratorImpl() throws BadCommandException {
        init();
    }

    @SuppressWarnings("unchecked")
    protected void init() throws BadCommandException {
        Exception ex = null;
        try {
            createCommand(CommandID.GET, GetCommand.class, new Class[]{
                        TimeoutFilter.class, FailOverFilter.class});
            createCommand(CommandID.SET, SetCommand.class, new Class[]{
                        TimeoutFilter.class, FailOverFilter.class});
            createCommand(CommandID.APPEND, AppendCommand.class, new Class[]{
                        TimeoutFilter.class, FailOverFilter.class});
            createCommand(CommandID.PREPEND, PrependCommand.class, new Class[]{
                        TimeoutFilter.class, FailOverFilter.class});
            createCommand(CommandID.DELETE, DeleteCommand.class, new Class[]{
                        TimeoutFilter.class, FailOverFilter.class});
            createCommand(CommandID.INCREMENT, IncrCommand.class, new Class[]{
                        TimeoutFilter.class, FailOverFilter.class});
            createCommand(CommandID.DECREMENT, DecrCommand.class, new Class[]{
                        TimeoutFilter.class, FailOverFilter.class});
            createCommand(CommandID.ROUTING_DUMP, RoutingdumpCommand.class,
                    new Class[]{TimeoutFilter.class});
            createCommand(CommandID.ROUTING_MKLHASH, RoutingmhtCommand.class,
                    new Class[]{TimeoutFilter.class});
        } catch (InstantiationException e) {
            ex = e;
        } catch (IllegalAccessException e) {
            ex = e;
        }

        if (ex != null) {
            throw new BadCommandException(ex);
        }
    }

    public Command getCommand(int commandID) {
        Command command = commands.get(new Integer(commandID));
        if (command == null) {
            throw new NullPointerException("command is not defined: #" + commandID);
        }
        return command;
    }

    public void createCommand(int commandID,
            Class<? extends Command> commandClass)
            throws InstantiationException, IllegalAccessException {
        Command command = (Command) commandClass.newInstance();
        commands.put(new Integer(commandID), command);
    }

    public void createCommand(int commandID,
            Class<? extends Command> commandClass,
            Class<? extends CommandFilter>[] filterClasses)
            throws InstantiationException, IllegalAccessException {
        Command command = (Command) commandClass.newInstance();
        for (int i = 0; i < filterClasses.length; ++i) {
            CommandFilter filter = (CommandFilter) filterClasses[i].newInstance();
            command = command.addFilter(filter);
        }
        commands.put(new Integer(commandID), command);
    }
}
