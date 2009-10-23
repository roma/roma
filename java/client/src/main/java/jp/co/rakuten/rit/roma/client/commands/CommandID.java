package jp.co.rakuten.rit.roma.client.commands;

/**
 * 
 */
public interface CommandID {

    // storage command
    int GET = 50;

    int SET = 51;

    int APPEND = 52;

    int PREPEND = 53;

    int DELETE = 54;

    int INCREMENT = 55;

    int DECREMENT = 56;

    String STR_CRLR = "\r\n";

    String STR_WHITE_SPACE = " ";

    String STR_GET = "get";

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
