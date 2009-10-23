package jp.co.rakuten.rit.roma.client.command;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public abstract class AbstractCommandFilter extends AbstractCommand implements
	CommandFilter {

    private Command next;

    void setCommand(Command command) {
	if (command == null) {
	    throw new IllegalArgumentException();
	}
	next = command;
    }

    @Override
    public Command addFilter(CommandFilter filter) {
	return super.addFilter(filter);
    }

    public abstract void preExecute(CommandContext context)
	    throws CommandException;

    public abstract void postExecute(CommandContext context)
	    throws CommandException;

    public abstract boolean aroundExecute(Command command,
	    CommandContext context) throws CommandException;

    @SuppressWarnings("unchecked")
    @Override
    public boolean execute(CommandContext context) throws CommandException {
	boolean executeRet = true;
	try {
	    preExecute(context);
	} catch (CommandException e) {
	    executeRet = false;
	    List<Exception> ex = (List<Exception>) context
		    .get(CommandContext.EXCEPTION);
	    if (ex == null) {
		ex = new ArrayList<Exception>();
	    }
	    ex.add(e);
	    context.put(CommandContext.EXCEPTION, ex);
	}

	if (executeRet) {
	    // executeRet = next.execute(context);
	    executeRet = aroundExecute(next, context);
	}

	try {
	    postExecute(context);
	} catch (CommandException e) {
	    executeRet = false;
	    List<Exception> ex = (List<Exception>) context
		    .get(CommandContext.EXCEPTION);
	    if (ex == null) {
		ex = new ArrayList<Exception>();
	    }
	    ex.add(e);
	    context.put(CommandContext.EXCEPTION, ex);
	}

	return executeRet;
    }
}
