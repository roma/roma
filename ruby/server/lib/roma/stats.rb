require 'singleton'
require 'roma/config'

module Roma

  class Stats
    include Singleton

    # command options
    attr_accessor :address, :port
    attr_accessor :daemon
    attr_accessor :join_ap
    attr_accessor :start_with_failover
    attr_accessor :name
    attr_accessor :verbose
    attr_accessor :enabled_repetition_host_in_routing

    # proc mode
    attr_accessor :enabled_vnodes_balance

    # proc status
    attr_accessor :run_acquire_vnodes
    attr_accessor :run_recover
    attr_accessor :run_sync_routing
    attr_accessor :run_iterate_storage
    attr_accessor :run_storage_clean_up
    attr_accessor :run_receive_a_vnode
    attr_accessor :run_release

    # proc param
    attr_accessor :stream_copy_wait_param

    # compressed redundant param
    attr_accessor :size_of_zredundant

    # batch schedule
    attr_accessor :crontab

    # performance counter
    attr_accessor :write_count
    attr_accessor :read_count
    attr_accessor :delete_count
    attr_accessor :out_count
    attr_accessor :out_message_count
    attr_accessor :redundant_count

    def initialize
      @run_acquire_vnodes = false
      @run_recover = false
      @run_sync_routing = false
      @run_iterate_storage = false
      @run_storage_clean_up = false
      @run_receive_a_vnode = false
      @run_release = false
      @stream_copy_wait_param = 
        Roma::Config::DATACOPY_STREAM_COPY_WAIT_PARAM
      @enabled_vnodes_balance = nil
      @write_count = 0
      @read_count = 0
      @delete_count = 0
      @out_count = 0
      @out_message_count = 0
      @redundant_count = 0
      @size_of_zredundant = 0
    end

    def load_crontab
      return nil unless File::exist?("#{ap_str}.crontab")
      buf=''
      open("#{ap_str}.crontab",'r'){|io|
        while((line=io.gets)!=nil)
          buf << line
        end
      }
      buf
    end

    def ap_str
      "#{@address}_#{port}"
    end

    def get_stat
      ret = {}
      ret['stats.address'] = @address
      ret['stats.port'] = @port
      ret['stats.daemon'] = @daemon
      ret['stats.name'] = @name
      ret['stats.verbose'] = @verbose
      ret['stats.enabled_repetition_host_in_routing'] = @enabled_repetition_host_in_routing
      ret['stats.run_acquire_vnodes'] = @run_acquire_vnodes
      ret['stats.run_recover'] = @run_recover
      ret['stats.run_sync_routing'] = @run_sync_routing
      ret['stats.run_iterate_storage'] = @run_iterate_storage
      ret['stats.run_storage_clean_up'] = @run_storage_clean_up
      ret['stats.run_release'] = @run_release
      ret['stats.stream_copy_wait_param'] = @stream_copy_wait_param
      ret['stats.size_of_zredundant'] = @size_of_zredundant
      ret['stats.write_count'] = @write_count
      ret['stats.read_count'] = @read_count
      ret['stats.delete_count'] = @delete_count
      ret['stats.out_count'] = @out_count
      ret['stats.out_message_count'] = @out_message_count
      ret['stats.redundant_count'] = @redundant_count
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

    private

    def clear_count(var)
      if self.instance_variable_get(var) > 0xffffffff
        self.instance_variable_set(var,0)
      end
    end

  end # class Stats

end # module Roma
