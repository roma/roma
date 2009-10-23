package jp.co.rakuten.rit.roma.client.commands;

/**
 * 
 */
public class SetCommand extends StoreCommand {

    @Override
    public String getCommand() throws BadCommandException {
	return STR_SET;
    }
}
