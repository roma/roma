package jp.co.rakuten.rit.roma.client.command;

import java.io.IOException;

import junit.framework.TestCase;

public class AbstractCommandTest extends TestCase {

    private static String KEY_PREFIX = AbstractCommandTest.class.getName();

    private static String KEY = null;

    public AbstractCommandTest() {
    }

    @Override
    public void setUp() {
    }

    @Override
    public void tearDown() {
	KEY = null;
    }

    public void testExecute01() throws CommandException {
	KEY = KEY_PREFIX + "testExecute01";
	Command command = new AbstractCommand() {
	    @Override
	    public boolean execute(CommandContext context) {
		String s = (String) context.get(KEY);
		if (s != null) {
		    s = s + "02";
		    context.put(KEY, s);
		    return true;
		} else {
		    return false;
		}
	    }
	};
	CommandContext context = new CommandContext();
	context.put(KEY, "01");
	boolean ret = command.execute(context);
	assertTrue(ret);
	assertEquals("0102", context.get(KEY));
    }

    public void testExecute02() throws Exception {
	Command command = new AbstractCommand() {
	    @Override
	    public boolean execute(CommandContext context) {
		return false;
	    }
	};
	CommandContext context = new CommandContext();
	boolean ret = command.execute(context);
	assertFalse(ret);
    }

    public void testExecute03() throws Exception {
	Command command = new AbstractCommand() {
	    @Override
	    public boolean execute(CommandContext context)
		    throws CommandException {
		throw new CommandException(new IOException("test"));
	    }
	};
	CommandContext context = new CommandContext();
	try {
	    command.execute(context);
	    fail();
	} catch (CommandException e) {
	    Throwable t = e.getCause();
	    assertTrue(t instanceof IOException);
	}
    }

    public void testAddFilter01() throws CommandException {
	KEY = KEY_PREFIX + "testAddFilter01";
	Command command = new AbstractCommand() {
	    @Override
	    public boolean execute(CommandContext context) {
		String s = (String) context.get(KEY);
		if (s != null) {
		    s = s + "01";
		    context.put(KEY, s);
		    return true;
		} else {
		    return false;
		}
	    }
	};
	CommandFilter filter01 = new AbstractCommandFilter() {
	    @Override
	    public void preExecute(CommandContext context)
		    throws CommandException {
		String s = (String) context.get(KEY);
		if (s != null) {
		    s = s + "02";
		} else {
		    s = s + "0X";
		}
		context.put(KEY, s);
	    }

	    @Override
	    public void postExecute(CommandContext context)
		    throws CommandException {
		String s = (String) context.get(KEY);
		if (s != null) {
		    s = s + "03";
		} else {
		    s = s + "0X";
		}
		context.put(KEY, s);
	    }

	    @Override
	    public boolean aroundExecute(Command command, CommandContext context)
		    throws CommandException {
		try {
		    String s = (String) context.get(KEY);
		    if (s != null) {
			s = s + "04";
		    } else {
			s = s + "0X";
		    }
		    context.put(KEY, s);
		    return command.execute(context);
		} finally {
		    String s = (String) context.get(KEY);
		    if (s != null) {
			s = s + "05";
		    } else {
			s = s + "0X";
		    }
		    context.put(KEY, s);
		}
	    }
	};

	Command command0 = command.addFilter(filter01);
	CommandContext context = new CommandContext();
	context.put(KEY, "");

	boolean ret = command0.execute(context);
	assertTrue(ret);
	String s = (String) context.get(KEY);
	assertTrue(s != null);
	assertEquals("0204010503", s);
    }

    public void testAddFilter02() throws CommandException {
	System.out.println("testAddFilter02");

	Command command = new AbstractCommand() {
	    @Override
	    public boolean execute(CommandContext context) {
		System.out.println("invoke command");
		return true;
	    }
	};
	CommandContext context = new CommandContext();
	CommandFilter filter01 = new AbstractCommandFilter() {
	    @Override
	    public void preExecute(CommandContext context)
		    throws CommandException {
		System.out.println("filter01 pre");
	    }

	    @Override
	    public void postExecute(CommandContext context)
		    throws CommandException {
		System.out.println("filter01 post");
	    }

	    @Override
	    public boolean aroundExecute(Command command, CommandContext context)
		    throws CommandException {
		try {
		    System.out.println("filter01 around pre");
		    return command.execute(context);
		} finally {
		    System.out.println("filter01 around post");
		}
	    }
	};
	CommandFilter filter02 = new AbstractCommandFilter() {
	    @Override
	    public void preExecute(CommandContext context)
		    throws CommandException {
		System.out.println("filter02 pre");
	    }

	    @Override
	    public void postExecute(CommandContext context)
		    throws CommandException {
		System.out.println("filter02 post");
	    }

	    @Override
	    public boolean aroundExecute(Command command, CommandContext context)
		    throws CommandException {
		try {
		    System.out.println("filter02 around pre");
		    return command.execute(context);
		} finally {
		    System.out.println("filter02 around post");
		}
	    }
	};
	command = command.addFilter(filter01);
	command = command.addFilter(filter02);
	boolean ret = command.execute(context);
	System.out.println("ret: " + ret);
    }
}
