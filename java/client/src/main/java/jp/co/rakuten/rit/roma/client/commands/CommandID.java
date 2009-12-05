package jp.co.rakuten.rit.roma.client.commands;

/**
 * 
 */
public interface CommandID {

    // storage command
    int GET = 50;
    
    int GETS = 51;
    
    int GETS_OPT = 52;
    
    int SET = 54;

    int APPEND = 55;

    int PREPEND = 56;

    int DELETE = 57;

    int INCREMENT = 58;

    int DECREMENT = 59;

    String STR_CRLF = "\r\n";

    String STR_WHITE_SPACE = " ";

    String STR_GET = "get";
    
    String STR_GETS = "gets";

    String STR_SET = "set";

    String STR_APPEND = "append";

    String STR_PREPEND = "prepend";

    String STR_DELETE = "delete";

    String STR_INCREMENT = "incr";

    String STR_DECREMENT = "decr";

    // routing table command
    int ROUTING_DUMP = 80;

    int ROUTING_MKLHASH = 81;

    int CLOSE = 82;

    String STR_ROUTING_DUMP = "routingdump";

    String STR_ROUTING_MKLHASH = "mklhash";

    String STR_CLOSE = "quit";

}
