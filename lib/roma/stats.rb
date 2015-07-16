require 'singleton'

module Roma

  class Stats
    include Singleton

    # environment options
    attr_accessor :config_path

    # command options
    attr_accessor :address, :port
    attr_accessor :daemon
    attr_accessor :join_ap
    attr_accessor :start_with_failover
    attr_accessor :name
    attr_accessor :verbose
    attr_accessor :enabled_repetition_host_in_routing
    alias :rep_host :enabled_repetition_host_in_routing
    attr_accessor :disabled_cmd_protect

    # proc mode
    attr_accessor :enabled_vnodes_balance

    # proc status
    attr_accessor :run_recover
    attr_accessor :run_sync_routing
    attr_accessor :run_iterate_storage
    attr_accessor :run_storage_clean_up
    attr_accessor :run_receive_a_vnode
    attr_accessor :run_release
    attr_accessor :run_join
    attr_accessor :run_balance

    attr_accessor :last_clean_up
    attr_accessor :spushv_protection

    # proc param
    attr_accessor :stream_copy_wait_param
    attr_accessor :stream_show_wait_param
    attr_accessor :dcnice
    attr_accessor :clean_up_interval

    # compressed redundant param
    attr_accessor :size_of_zredundant

    # performance counter
    attr_accessor :write_count
    attr_accessor :read_count
    attr_accessor :delete_count
    attr_accessor :out_count
    attr_accessor :out_message_count
    attr_accessor :redundant_count

    attr_accessor :hilatency_warn_time

    # for write behind
    attr_accessor :wb_command_map

    # for latency average check
    attr_accessor :latency_log
    attr_accessor :latency_check_cmd
    attr_accessor :latency_check_time_count
    attr_accessor :latency_data
    #attr_accessor :latency_denominator

    # for vnode copy parameter
    attr_accessor :spushv_klength_warn
    attr_accessor :spushv_vlength_warn
    attr_accessor :spushv_read_timeout
    attr_accessor :reqpushv_timeout_count

    attr_accessor :routing_trans_timeout

    # for GUI tool
    attr_accessor :gui_run_snapshot
    attr_accessor :gui_run_gather_logs
    attr_accessor :gui_last_snapshot

    # for log
    attr_accessor :log_shift_size
    attr_accessor :log_shift_age
    attr_accessor :log_level

    def initialize
      @config_path = nil
      @run_recover = false
      @run_sync_routing = false
      @run_iterate_storage = false
      @run_storage_clean_up = false
      @run_receive_a_vnode = {}
      @run_release = false
      @run_join = false
      @run_balance = false
      @gui_run_snapshot = false
      @gui_run_gather_logs = false
      @last_clean_up = Time.now
      @gui_last_snapshot = []
      @spushv_protection = false
      @stream_copy_wait_param = 0.0001
      @stream_show_wait_param = 0.001
      @dcnice = 3
      @clean_up_interval = 300
      @enabled_vnodes_balance = nil
      @write_count = 0
      @read_count = 0
      @delete_count = 0
      @out_count = 0
      @out_message_count = 0
      @redundant_count = 0
      @size_of_zredundant = 0
      @hilatency_warn_time = 5.0
      @wb_command_map = {}
      @latency_log = false
      @latency_check_cmd =["get", "set", "delete"]
      @latency_check_time_count = false
      @latency_data = Hash.new { |hash,key| hash[key] = {} } #double hash
      @spushv_klength_warn = 1024 # 1kB
      @spushv_vlength_warn = 1024 * 1024 # 1MB
      @spushv_read_timeout = 100
      @reqpushv_timeout_count = 300 # 0.1 * 300 sec
      @routing_trans_timeout = 3600 * 3 # 3hr
      @log_shift_size = 1048576
      @log_shift_age = 0
      @log_level = :debug
    end

    def ap_str
      "#{@address}_#{port}"
    end

    def get_stat
      ret = {}
      ret['stats.config_path'] = @config_path
      ret['stats.address'] = @address
      ret['stats.port'] = @port
      ret['stats.daemon'] = @daemon
      ret['stats.name'] = @name
      ret['stats.verbose'] = @verbose
      ret['stats.enabled_repetition_host_in_routing'] = rep_host
      ret['stats.run_recover'] = @run_recover
      ret['stats.run_sync_routing'] = @run_sync_routing
      ret['stats.run_iterate_storage'] = @run_iterate_storage
      ret['stats.run_storage_clean_up'] = @run_storage_clean_up
      ret['stats.run_receive_a_vnode'] = @run_receive_a_vnode.inspect
      ret['stats.run_release'] = @run_release
      ret['stats.run_join'] = @run_join
      ret['stats.run_balance'] = @run_balance
      ret['stats.gui_run_snapshot'] = @gui_run_snapshot
      ret['stats.last_clean_up'] = @last_clean_up
      ret['stats.gui_last_snapshot'] = @gui_last_snapshot
      ret['stats.spushv_protection'] = @spushv_protection
      ret['stats.stream_copy_wait_param'] = @stream_copy_wait_param
      ret['stats.stream_show_wait_param'] = @stream_show_wait_param
      ret['stats.dcnice'] = @dcnice
      ret['stats.clean_up_interval'] = @clean_up_interval
      ret['stats.size_of_zredundant'] = @size_of_zredundant
      ret['stats.write_count'] = @write_count
      ret['stats.read_count'] = @read_count
      ret['stats.delete_count'] = @delete_count
      ret['stats.out_count'] = @out_count
      ret['stats.out_message_count'] = @out_message_count
      ret['stats.redundant_count'] = @redundant_count
      ret['stats.hilatency_warn_time'] = @hilatency_warn_time
      ret['stats.wb_command_map'] = @wb_command_map.inspect
      ret['stats.latency_log']  = @latency_log
      ret['stats.latency_check_cmd']  = @latency_check_cmd
      ret['stats.latency_check_time_count']  = @latency_check_time_count
      ret['stats.spushv_klength_warn'] = @spushv_klength_warn
      ret['stats.spushv_vlength_warn'] = @spushv_vlength_warn
      ret['stats.spushv_read_timeout'] = @spushv_read_timeout
      ret['stats.reqpushv_timeout_count'] = @reqpushv_timeout_count
      ret['stats.routing_trans_timeout'] = @routing_trans_timeout
      ret['stats.log_shift_size'] = @log_shift_size
      ret['stats.log_shift_age'] = @log_shift_age
      ret['stats.log_level'] = @log_level
      ret
    end

    def clear_counters
      clear_count(:@write_count)
      clear_count(:@read_count)
      clear_count(:@delete_count)
      clear_count(:@out_count)
      clear_count(:@out_message_count)
      clear_count(:@redundant_count)
    end

    def do_clean_up?
      @last_clean_up.to_i + @clean_up_interval < Time.now.to_i
    end

    private

    def clear_count(var)
      if self.instance_variable_get(var) > 0xffffffff
        self.instance_variable_set(var,0)
      end
    end

  end # class Stats

end # module Roma
