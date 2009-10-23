package jp.co.rakuten.rit.roma.client.commands;

/**
 * 
 */
public class DecrCommand extends IncrAndDecrCommand implements CommandID {

    @Override
    public String getCommand() throws BadCommandException {
	return STR_DECREMENT;
    }
}
