package jp.co.rakuten.rit.roma.client;

/**
 * 
 */
public class ClientException extends Exception {

    private static final long serialVersionUID = -7032182229500031385L;

    public ClientException() {
        super();
    }

    public ClientException(Throwable cause) {
        super(cause);
    }

    public ClientException(String reason) {
        super(reason);
    }

    public ClientException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
