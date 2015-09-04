require 'roma/storage/rh_storage'
require 'roma/storage/storage_error_storage'

module Roma
  
  module Config
    VERSION = "1.3.0"

    DEFAULT_PORT = 12000
    DEFAULT_NAME = 'ROMA'

    # :no_action | :auto_assign | :shutdown
    DEFAULT_LOST_ACTION = :auto_assign

    # log setting
    LOG_SHIFT_AGE = 10
    LOG_SHIFT_SIZE = 1024 * 1024 * 10
    LOG_PATH = '.'
    # :debug | :info | :warn | :error
    LOG_LEVEL = :debug

    # routing setting
    RTTABLE_PATH = '.'

    # connection setting
    # like a MaxStartups spec in the sshd_config
    # 'start:rate:full'
    CONNECTION_CONTINUOUS_LIMIT = '200:30:300'

    # storage setting
#    STORAGE_CLASS = Roma::Storage::RubyHashStorage
    STORAGE_CLASS = Storage::StorageErrorStorage
    STORAGE_DIVNUM = 10
    STORAGE_PATH = '.'
    STORAGE_DUMP_PATH = '/tmp'
    STORAGE_OPTION = ''

    # 5 days ago
    STORAGE_DELMARK_EXPTIME = 60 * 60 * 24 * 5

    # data copy setting
    DATACOPY_STREAM_COPY_WAIT_PARAM = 0.0001

    # plugin setting
    PLUGIN_FILES = ['plugin_storage.rb','plugin_alist.rb','plugin_map.rb']

    # write-behind setting
    WRITEBEHIND_PATH = './wb'
    WRITEBEHIND_SHIFT_SIZE = 1024 * 1024 * 10

    # redundant setting
    REDUNDANT_ZREDUNDANT_SIZE = 0

    def self.get_stat
      ret = {}
      ret['config.DEFAULT_LOST_ACTION'] = DEFAULT_LOST_ACTION
      ret['config.LOG_SHIFT_AGE'] = LOG_SHIFT_AGE
      ret['config.LOG_SHIFT_SIZE'] = LOG_SHIFT_SIZE
      ret['config.LOG_PATH'] = File.expand_path(LOG_PATH)
      ret['config.RTTABLE_PATH'] = File.expand_path(RTTABLE_PATH)
      ret['config.STORAGE_DELMARK_EXPTIME'] = STORAGE_DELMARK_EXPTIME
      ret['config.DATACOPY_STREAM_COPY_WAIT_PARAM'] = DATACOPY_STREAM_COPY_WAIT_PARAM
      ret['config.PLUGIN_FILES'] = PLUGIN_FILES.inspect
      ret['config.WRITEBEHIND_PATH'] = File.expand_path(WRITEBEHIND_PATH)
      ret['config.WRITEBEHIND_SHIFT_SIZE'] = WRITEBEHIND_SHIFT_SIZE
      ret
    end

  end

end
