require 'roma/storage/rh_storage'

module Roma
  
  module Config
    # check when the booting
    VERSION = "1.2.0"

    DEFAULT_PORT = 12000
    DEFAULT_NAME = 'ROMA'

    # :no_action | :auto_assign | :shutdown
    DEFAULT_LOST_ACTION = :auto_assign
    # to use a auto recover function, AUTO_RECOVER is to set true
    AUTO_RECOVER = false

    # failover setting
    # threshold of failover occurrence
    ROUTING_FAIL_CNT_THRESHOLD = 15
    # ROUTING_FAIL_CNT_GAP(sec) doesn't increase the failover counter.
    ROUTING_FAIL_CNT_GAP = 0

    # log setting
    LOG_SHIFT_AGE = 10
    LOG_SHIFT_SIZE = 1024 * 1024 * 10
    LOG_PATH = '.'
    # :debug | :info | :warn | :error
    LOG_LEVEL = :debug

    # routing setting
    RTTABLE_PATH = '.'
    ROUTING_EVENT_LIMIT_LINE = 1000

    # connection setting

    # to use a system call of epoll, CONNECTION_USE_EPOLL is to set true
    CONNECTION_USE_EPOLL = true
    # to use a system call of epoll, CONNECTION_DESCRIPTOR_TABLE_SIZE can be setting
    CONNECTION_DESCRIPTOR_TABLE_SIZE = 4096

    # like a MaxStartups spec in the sshd_config
    # 'start:rate:full'
    CONNECTION_CONTINUOUS_LIMIT = '200:30:300'
    # expired time(sec) for accepted connections
    CONNECTION_EXPTIME = 0

    # expired time(sec) for an async connection in the connection pool
    # CONNECTION_POOL_EXPTIME should be less than CONNECTION_EXPTIME
    CONNECTION_POOL_EXPTIME = 30
    # max length of the connection pool
    CONNECTION_POOL_MAX = 5

    # expired time(sec) for an eventmachine's connection in the connection pool
    # CONNECTION_EMPOOL_EXPTIME should be less than CONNECTION_EXPTIME
    CONNECTION_EMPOOL_EXPTIME = 30
    # max length of the eventmachine's connection pool
    CONNECTION_EMPOOL_MAX = 15

    # lookup hostname to the nameserver everytime or only first time.
    # if set true, the result of lookup will keep.
    DNS_CACHING = false

    # storage setting
    STORAGE_CLASS = Roma::Storage::RubyHashStorage
    STORAGE_DIVNUM = 10
    STORAGE_PATH = '.'
    STORAGE_DUMP_PATH = '/tmp'
    STORAGE_OPTION = ''
    # :no_action | :shutdown
    STORAGE_EXCEPTION_ACTION = :no_action

    # expired time(sec) for deleted keys, expired keys and invalid vnode keys
    # typical value is 5 days
    STORAGE_DELMARK_EXPTIME = 60 * 60 * 24 * 5

    STORAGE_CLEAN_UP_INTERVAL = 60 * 5

    # data copy setting
    DATACOPY_STREAM_COPY_WAIT_PARAM = 0.001

    # show logs setting
    LOG_STREAM_SHOW_WAIT_PARAM = 0.001

    # plugin setting
    PLUGIN_FILES = ['plugin_storage.rb']

    # write-behind setting
    WRITEBEHIND_PATH = './wb'
    WRITEBEHIND_SHIFT_SIZE = 1024 * 1024 * 10

    # redundant setting
    # REDUNDANT_ZREDUNDANT_SIZE is a option for a redundancy of compressed data.
    # when the data size is more then REDUNDANT_ZREDUNDANT_SIZE, data compression is done.
    # however, it does't in case of REDUNDANT_ZREDUNDANT_SIZE is zero.
    REDUNDANT_ZREDUNDANT_SIZE = 0

  end

end
