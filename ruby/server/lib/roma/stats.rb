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
      @last_clean_up = Time.now
      @spushv_protection = false
      @stream_copy_wait_param = 0.0001
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
      @hilatency_warn_time = 5
      @wb_command_map = {}
      @latency_log = false
      @latency_check_cmd =["get", "set", "delete"]
      @latency_check_time_count = nil
      @latency_data = Hash.new { |hash,key| hash[key] = {} } #double hash
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
      ret['stats.last_clean_up'] = @last_clean_up
      ret['stats.spushv_protection'] = @spushv_protection
      ret['stats.stream_copy_wait_param'] = @stream_copy_wait_param
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
