package jp.co.rakuten.rit.roma.client.command;

import junit.framework.TestCase;

public class CommandChainTest extends TestCase {
    public void testAddCommand01() throws Exception {
        System.out.println("testAddCommand01");

        CommandChain chain = new CommandChain();
        Command command01 = new AbstractCommand() {
            @Override public boolean execute(CommandContext context) {
                System.out.println("invoke Command01#execute()");
                return true;
            }
        };
        chain.addCommand(command01);
        CommandContext context = new CommandContext();
        boolean ret = chain.execute(context);
        System.out.println("ret: " + ret);

    }

    public void testAddCommand02() throws Exception {
        System.out.println("testAddCommand02");

        CommandChain chain = new CommandChain();
        Command command01 = new AbstractCommand() {
            @Override public boolean execute(CommandContext context) {
                System.out.println("invoke Command01#execute()");
                return true;
            }
        };
        Command command02 = new AbstractCommand() {
            @Override public boolean execute(CommandContext context) {
                System.out.println("invoke Command02#execute()");
                return true;
            }
        };
        chain.addCommand(command01);
        chain.addCommand(command02);
        CommandContext context = new CommandContext();
        boolean ret = chain.execute(context);
        System.out.println("ret: " + ret);

    }

    public void testAddCommand03() throws Exception {
        System.out.println("testAddCommand03");

        CommandChain chain = new CommandChain();
        CommandContext context = new CommandContext();
        boolean ret = chain.execute(context);
        System.out.println("ret: " + ret);

    }
}
