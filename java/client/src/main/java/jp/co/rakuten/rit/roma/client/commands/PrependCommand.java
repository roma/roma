package jp.co.rakuten.rit.roma.client.commands;

/**
 * 
 */
public class PrependCommand extends StoreCommand implements CommandID {
    @Override
    public String getCommand() throws BadCommandException {
	return STR_PREPEND;
    }
}
