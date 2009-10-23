package jp.co.rakuten.rit.roma.client.util.commands;

import jp.co.rakuten.rit.roma.client.commands.CommandID;

/**
 * 
 */
public interface ListCommandID extends CommandID {

    // storage command
    int ALIST_AT = 100;

    int ALIST_CLEAR = 101;

    int ALIST_DELETE = 102;

    int ALIST_DELETE_AT = 103;

    int ALIST_EMPTY = 104;

    int ALIST_FIRST = 105;

    int ALIST_INCLUDE = 106;

    int ALIST_INDEX = 107;

    int ALIST_INSERT = 108;

    int ALIST_SIZED_INSERT = 109;

    int ALIST_SWAP_AND_INSERT = 110;

    int ALIST_SWAP_AND_SIZED_INSERT = 111;
    
    int ALIST_EXPIRED_SWAP_AND_INSERT = 127;
    
    int ALIST_EXPIRED_SWAP_AND_SIZED_INSERT = 128;

    int ALIST_LAST = 112;

    int ALIST_LENGTH = 113;

    int ALIST_POP = 114;

    int ALIST_PUSH = 115;

    int ALIST_SIZED_PUSH = 116;

    int ALIST_SWAP_AND_PUSH = 117;

    int ALIST_SWAP_AND_SIZED_PUSH = 118;

    int ALIST_EXPIRED_SWAP_AND_PUSH = 125;

    int ALIST_EXPIRED_SWAP_AND_SIZED_PUSH = 126;

    int ALIST_SHIFT = 119;

    int ALIST_TO_S = 120;

    int ALIST_JOIN = 121;

    int ALIST_JOIN_WITH_TIME = 122;

    int ALIST_GETS = 123;

    int ALIST_GETS_WITH_TIME = 124;

    String STR_ALIST_AT = "alist_at";

    String STR_ALIST_CLEAR = "alist_clear";

    String STR_ALIST_DELETE = "alist_delete";

    String STR_ALIST_DELETE_AT = "alist_delete_at";

    String STR_ALIST_EMPTY = "alist_empty?";

    String STR_ALIST_FIRST = "alist_first";

    String STR_ALIST_INCLUDE = "alist_include?";

    String STR_ALIST_INDEX = "alist_index";

    String STR_ALIST_INSERT = "alist_insert";

    String STR_ALIST_SIZED_INSERT = "alist_sized_insert";

    String STR_ALIST_SWAP_AND_INSERT = "alist_swap_and_insert";

    String STR_ALIST_SWAP_AND_SIZED_INSERT = "alist_swap_and_sized_insert";

    String STR_ALIST_EXPIRED_SWAP_AND_INSERT = "alist_expired_swap_and_insert";
    
    String STR_ALIST_EXPIRED_SWAP_AND_SIZED_INSERT = "alist_expired_swap_and_sized_insert";
    
    String STR_ALIST_LAST = "alist_last";

    String STR_ALIST_LENGTH = "alist_length";

    String STR_ALIST_POP = "alist_pop";

    String STR_ALIST_PUSH = "alist_push";

    String STR_ALIST_SIZED_PUSH = "alist_sized_push";

    String STR_ALIST_SWAP_AND_PUSH = "alist_swap_and_push";

    String STR_ALIST_SWAP_AND_SIZED_PUSH = "alist_swap_and_sized_push";

    String STR_ALIST_EXPIRED_SWAP_AND_PUSH = "alist_expired_swap_and_push";

    String STR_ALIST_EXPIRED_SWAP_AND_SIZED_PUSH = "alist_expired_swap_and_sized_push";

    String STR_ALIST_SHIFT = "alist_shift";

    String STR_ALIST_TO_S = "alist_to_s";

    String STR_ALIST_JOIN = "alist_join";

    String STR_ALIST_JOIN_WITH_TIME = "alist_join_with_time";

    String STR_ALIST_GETS = "alist_gets";

    String STR_ALIST_GETS_WITH_TIME = "alist_gets_with_time";

}
