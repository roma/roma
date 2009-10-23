package jp.co.rakuten.rit.roma.client.commands;

/**
 * 
 */
public class AppendCommand extends StoreCommand implements CommandID {
    @Override
    public String getCommand() throws BadCommandException {
	return STR_APPEND;
    }

}
