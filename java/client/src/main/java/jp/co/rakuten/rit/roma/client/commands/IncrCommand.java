package jp.co.rakuten.rit.roma.client.commands;

/**
 * 
 */
public class IncrCommand extends IncrAndDecrCommand {

    @Override
    public String getCommand() throws BadCommandException {
	return STR_INCREMENT;
    }
}
