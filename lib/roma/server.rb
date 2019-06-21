#!/usr/bin/env ruby
require 'roma/version'
require 'roma/stats'
require 'roma/command_plugin'
require 'roma/async_process'
require 'roma/write_behind'
require 'roma/logging/rlogger'
require 'roma/command/receiver'
require 'roma/messaging/con_pool'
require 'roma/event/con_pool'
require 'roma/routing/churn_based_routing_table'
require 'timeout'

module Roma
  class Server
    include AsyncProcess
    include WriteBehindProcess

    DEFAULT_NAME = 'ROMA'.freeze
    DEFAULT_PORT = 12000.freeze
    DEFAULT_PID_DIR = './tmp/pids'.freeze

    attr :storages
    attr :routing_table
    attr :stats
    attr :wb_writer
    attr :cr_writer

    attr_accessor :eventloop
    attr_accessor :startup

    def initialize(address, options = {})
      @startup = true
      options[:address] ||= address
      @stats = Roma::Stats.instance
      initialize_stats(options)
      initialize_connection
      initialize_logger
      initialize_routing_table
      initialize_storages
      initialize_handler
      initialize_plugin
      initialize_wb_writer
    end

    def start
      validate_version_number_in_config

      if node_check(@stats.ap_str)
        @logger.error("#{@stats.ap_str} is already running.")
        return
      end

      @storages.each { |_, storage| storage.opendb }

      check_pid!
      if daemon?
        daemonize
        write_pid
      end

      start_async_process
      start_wb_process
      timer

      if @stats.join_ap
        AsyncProcess::queue.push(AsyncMessage.new('start_join_process'))
      end

      # select a kind of system call
      if Config.const_defined?(:CONNECTION_USE_EPOLL) && Config::CONNECTION_USE_EPOLL
        @logger.info("use an epoll")
        EM.epoll
        if Config.const_defined?(:CONNECTION_DESCRIPTOR_TABLE_SIZE)
          EM.set_descriptor_table_size(Config::CONNECTION_DESCRIPTOR_TABLE_SIZE)
        end
      else
        @logger.info("use a select")
      end

      @eventloop = true
      while(@eventloop)
        @eventloop = false
        begin
          # initialize an instance of connections as restarting of an evantmachine
          Event::Handler::connections.each_key{|k|
            begin
              k.close_connection
            rescue Exception => e
              @logger.error("#{e}\n#{$@}")
            end
          }
          Event::Handler::connections.clear

          EventMachine::run do
            EventMachine.start_server('0.0.0.0', @stats.port,
                                      Roma::Command::Receiver,
                                      @storages, @routing_table)
            @logger.info("Roma server established: #{@stats.address}:#{@stats.port}")
            # a management of connections lives
            EventMachine::add_periodic_timer( 10 ) {
              if Event::Handler::connection_expire_time > 0
                dellist = []
                Event::Handler::connections.each{|k,v|
                  if k.connected == false || k.last_access == nil
                    dellist << k
                  elsif k.last_access < Time.now - Event::Handler::connection_expire_time
                    begin
                      k.close_connection
                      if k.addr
                        @logger.info("connection expired from #{k.addr}:#{k.port},lastcmd = #{k.lastcmd}")
                      else
                        @logger.info("connection expired in irregular connection")
                        dellist << k
                      end
                    rescue Exception => e
                      @logger.error("#{e}\n#{$@}")
                      dellist << k
                    end
                  end
                }
                dellist.each{|k|
                  @logger.info("delete connection lastcmd = #{k.lastcmd}")
                  Event::Handler::connections.delete(k)
                }
              end
            }

            @logger.info("Now accepting connections on address #{@stats.address}, port #{@stats.port}")
          end
        rescue Interrupt => e
          if daemon?
            @logger.error("#{e.inspect}\n#{$@}")
            retry
          else
            $stderr.puts "#{e.inspect}"
          end
        rescue Exception => e
          @logger.error("#{e}\n#{$@}")
          @logger.error("restart an eventmachine")
          retry
        end
      end
      stop_async_process
      stop_wb_process
      stop
    end

    def daemon?
      @stats.daemon
    end

    def stop_clean_up
      @stats.last_clean_up = Time.now
      while(@stats.run_storage_clean_up)
        @logger.info("Storage clean up process will be stop.")
        @storages.each_value{|st| st.stop_clean_up}
        sleep 0.005
      end
    end

    private

    def initialize_stats(options)
      @stats.daemon = options[:daemon]
      @stats.join_ap = options[:join]
      @stats.enabled_repetition_host_in_routing = true if options[:enabled_repeathost]
      @stats.enabled_repetition_host_in_routing = true if options[:replication_in_host]
      @stats.disabled_cmd_protect = options[:disabled_cmd_protect]
      if options[:config]
        @stats.config_path = File.expand_path(options[:config])
      else
        @stats.config_path = 'roma/config'
      end

      unless require @stats.config_path
        STDERR.puts "The given configuration file has been already required: #{@stats.config_path}"
      end

      @stats.address = options[:address]
      @stats.port = options[:port] || Config::DEFAULT_PORT
      @stats.name = options[:name] || Config::DEFAULT_NAME

      if @stats.join_ap
        @stats.join_ap = @stats.join_ap.sub(':', '_')
        raise "[address:port] can not be parsed." if !(@stats.join_ap =~ /^.+_\d+$/)
      end

      @stats.verbose = options[:verbose] unless options[:verbose].nil?

      if Config.const_defined?(:REDUNDANT_ZREDUNDANT_SIZE)
        @stats.size_of_zredundant = Config::REDUNDANT_ZREDUNDANT_SIZE
      end
      if Config.const_defined?(:DATACOPY_STREAM_COPY_WAIT_PARAM)
        @stats.stream_copy_wait_param = Config::DATACOPY_STREAM_COPY_WAIT_PARAM
      end
      if Config.const_defined?(:LOG_STREAM_SHOW_WAIT_PARAM)
        @stats.stream_show_wait_param = Config::LOG_STREAM_SHOW_WAIT_PARAM
      end
      if Config.const_defined?(:WB_COMMAND_MAP)
        @stats.wb_command_map = Config::WB_COMMAND_MAP
      end
      if Config.const_defined?(:STORAGE_CLEAN_UP_INTERVAL)
        @stats.clean_up_interval = Config::STORAGE_CLEAN_UP_INTERVAL
      end
      if Config.const_defined?(:LOG_SHIFT_SIZE)
        @stats.log_shift_size = Config::LOG_SHIFT_SIZE
      end
      if Config.const_defined?(:LOG_SHIFT_AGE)
        @stats.log_shift_age = Config::LOG_SHIFT_AGE
      end
      if Config.const_defined?(:LOG_LEVEL)
        @stats.log_level = Config::LOG_LEVEL
      end
    end

    def initialize_connection
      if Config.const_defined?(:CONNECTION_CONTINUOUS_LIMIT)
        unless Event::Handler.set_ccl(Config::CONNECTION_CONTINUOUS_LIMIT)
          raise "config parse error : Config::CONNECTION_CONTINUOUS_LIMIT"
        end
      end

      if Config.const_defined?(:CONNECTION_EXPTIME)
        Event::Handler::connection_expire_time = Config::CONNECTION_EXPTIME
      end

      if Config.const_defined?(:CONNECTION_POOL_EXPTIME)
        Messaging::ConPool.instance.expire_time = Config::CONNECTION_POOL_EXPTIME
      end

      if Config.const_defined?(:CONNECTION_POOL_MAX)
        Messaging::ConPool.instance.maxlength = Config::CONNECTION_POOL_MAX
      end

      if Config.const_defined?(:CONNECTION_EMPOOL_EXPTIME)
        Event::EMConPool::instance.expire_time = Config::CONNECTION_EMPOOL_EXPTIME
      end

      if Config.const_defined?(:CONNECTION_EMPOOL_MAX)
        Event::EMConPool::instance.maxlength = Config::CONNECTION_EMPOOL_MAX
      end
    end

    def initialize_wb_writer
      @wb_writer = Roma::WriteBehind::FileWriter.new(
                                                     Roma::Config::WRITEBEHIND_PATH,
                                                     Roma::Config::WRITEBEHIND_SHIFT_SIZE,
                                                     @loggger)

      @cr_writer = Roma::WriteBehind::StreamWriter.new(@loggger)
    end

    def initialize_plugin
      return unless Roma::Config.const_defined? :PLUGIN_FILES

      Roma::Config::PLUGIN_FILES.each do|f|
        require "roma/plugin/#{f}"
        @logger.info("roma/plugin/#{f} loaded")
      end
      Roma::CommandPlugin.plugins.each do |plugin|
          Roma::Command::Receiver.class_eval do
            include plugin
          end
          @logger.info("#{plugin.to_s} included")
      end

      if @stats.disabled_cmd_protect
        Command::Receiver::mk_evlist
      end
    end

    def initialize_handler
      if @stats.verbose
        Event::Handler.class_eval{
          alias gets2 gets
          undef gets

          def gets
            ret = gets2
            @logger.info("command log:#{ret.chomp}") if ret
            ret
          end
        }
      end

      if @stats.join_ap
        Command::Receiver::mk_evlist
      else
        Command::Receiver::mk_starting_evlist
      end
    end

    def initialize_logger
      Roma::Logging::RLogger.create_singleton_instance("#{Roma::Config::LOG_PATH}/#{@stats.ap_str}.log",
                                                       Roma::Config::LOG_SHIFT_AGE,
                                                       Roma::Config::LOG_SHIFT_SIZE)
      @logger = Roma::Logging::RLogger.instance

      if Config.const_defined? :LOG_LEVEL
        case Config::LOG_LEVEL
        when :debug
          @logger.level = Roma::Logging::RLogger::Severity::DEBUG
        when :info
          @logger.level = Roma::Logging::RLogger::Severity::INFO
        when :warn
          @logger.level = Roma::Logging::RLogger::Severity::WARN
        when :error
          @logger.level = Roma::Logging::RLogger::Severity::ERROR
        end
      end
    end

    def initialize_storages
      @storages = {}
      if Config.const_defined? :STORAGE_PATH
        path = "#{Roma::Config::STORAGE_PATH}/#{@stats.ap_str}"
      end

      if Config.const_defined? :STORAGE_CLASS
        st_class = Config::STORAGE_CLASS
      end

      if Config.const_defined? :STORAGE_DIVNUM
        st_divnum = Config::STORAGE_DIVNUM
      end
      if Config.const_defined? :STORAGE_OPTION
        st_option = Config::STORAGE_OPTION
      end

      path ||= './'
      st_class ||= Storage::RubyHashStorage
      st_divnum ||= 10
      st_option ||= nil
      Dir.glob("#{path}/*").each do |f|
        next unless File.directory?(f)
        hname = File.basename(f)
        st = st_class.new
        st.storage_path = "#{path}/#{hname}"
        st.vn_list = @routing_table.vnodes
        st.st_class = st_class
        st.divnum = st_divnum
        st.option = st_option
        @storages[hname] = st
      end

      if @storages.empty?
        hname = 'roma'
        st = st_class.new
        st.storage_path = "#{path}/#{hname}"
        st.vn_list = @routing_table.vnodes
        st.st_class = st_class
        st.divnum = st_divnum
        st.option = st_option
        @storages[hname] = st
      end
    end

    def initialize_routing_table
      if @stats.join_ap
        initialize_routing_table_join
      else
        file_path = "#{Roma::Config::RTTABLE_PATH}/#{@stats.ap_str}.route"
        raise "#{file_path} not found." unless File::exist?(file_path)
        routing_data = Roma::Routing::RoutingData::load(file_path)
        raise "It failed in loading the routing table data." unless routing_data
        if Config.const_defined? :RTTABLE_CLASS
          @routing_table = Config::RTTABLE_CLASS.new(routing_data, file_path)
        else
          @routing_table = Roma::Routing::ChurnbasedRoutingTable.new(routing_data, file_path)
        end
      end

      if Roma::Config.const_defined?(:RTTABLE_SUB_NID)
        @routing_table.sub_nid = Roma::Config::RTTABLE_SUB_NID
      end

      if Roma::Config.const_defined?(:ROUTING_FAIL_CNT_THRESHOLD)
        @routing_table.fail_cnt_threshold = Roma::Config::ROUTING_FAIL_CNT_THRESHOLD
      end
      if Roma::Config.const_defined?(:ROUTING_FAIL_CNT_GAP)
        @routing_table.fail_cnt_gap = Roma::Config::ROUTING_FAIL_CNT_GAP
      end
      @routing_table.lost_action = Roma::Config::DEFAULT_LOST_ACTION
      @routing_table.auto_recover = Roma::Config::AUTO_RECOVER if defined?(Roma::Config::AUTO_RECOVER)

      @routing_table.enabled_failover = false
      @routing_table.set_leave_proc{|nid|
        Roma::Messaging::ConPool.instance.close_same_host(nid)
        Roma::Event::EMConPool.instance.close_same_host(nid)
        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('broadcast_cmd',["leave #{nid}",[@stats.ap_str,nid,5]]))
      }
      @routing_table.set_lost_proc{
        if @routing_table.lost_action == :shutdown
          async_broadcast_cmd("rbalse lose_data\r\n")
          EventMachine::stop_event_loop
          @logger.error("Roma server has stopped, so that lose data.")
        end
      }
      @routing_table.set_recover_proc{|action|
        if (@routing_table.lost_action == :shutdown || @routing_table.lost_action == :auto_assign) && @routing_table.auto_recover == true
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new("#{action}"))
        elsif
          @logger.error("AUTO_RECOVER is off or Unavailable value is set to [DEFAULT_LOST_ACTION] => #{@routing_table.lost_action}")
        end
      }

      if Roma::Config.const_defined?(:ROUTING_EVENT_LIMIT_LINE)
        @routing_table.event_limit_line = Roma::Config::ROUTING_EVENT_LIMIT_LINE
      end
      Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_get_routing_event'))
    end

    def initialize_routing_table_join
      name = async_send_cmd(@stats.join_ap,"whoami\r\n")
      unless name
        raise "No respons from #{@stats.join_ap}."
      end

      if name != @stats.name
        raise "#{@stats.join_ap} has diffarent name.\n" +
          "me = \"#{@stats.name}\"  #{@stats.join_ap} = \"#{name}\""
      end

      file_path = "#{Roma::Config::RTTABLE_PATH}/#{@stats.ap_str}.route"
      if routing_dump = get_routedump(@stats.join_ap)
        routing_dump.save(file_path)
      else
        raise "It failed in getting the routing table data from #{@stats.join_ap}."
      end

      if routing_dump.nodes.include?(@stats.ap_str)
        raise "ROMA has already contained #{@stats.ap_str}."
      end

      @routing_table = Roma::Routing::ChurnbasedRoutingTable.new(routing_dump,file_path)
      nodes = @routing_table.nodes

      nodes.each{|nid|
        begin
          con = Roma::Messaging::ConPool.instance.get_connection(nid)
          con.write("join #{@stats.ap_str}\r\n")
          if con.gets != "ADDED\r\n"
            raise "Hotscale initialize failed.\n#{nid} is busy."
          end
          Roma::Messaging::ConPool.instance.return_connection(nid, con)
        rescue => e
          raise "Hotscale initialize failed.\n#{nid} unreachable connection."
        end
      }
      @routing_table.add_node(@stats.ap_str)
    end

    def get_routedump(nid)
      rcv = receive_routing_dump(nid, "routingdump bin\r\n")
      unless rcv
        rcv = receive_routing_dump(nid, "routingdump\r\n")
        routing_dump = Marshal.load(rcv)
      else
        routing_dump = Routing::RoutingData.decode_binary(rcv)
      end
      routing_dump
    rescue
      nil
    end

    def receive_routing_dump(nid, cmd)
      con = Messaging::ConPool.instance.get_connection(nid)
      con.write(cmd)
      unless select [con], nil, nil, 1
        con.close
        return nil
      end
      len = con.gets
      if len.to_i <= 0
        con.close
        return nil
      end

      rcv=''
      while(rcv.length != len.to_i)
        rcv = rcv + con.read(len.to_i - rcv.length)
      end
      con.read(2)
      con.gets
      Messaging::ConPool.instance.return_connection(nid,con)
      rcv
    rescue Exception
      nil
    end

    def timer
      t = Thread.new do
        loop do
          sleep 1
          timer_event_1sec
        end
      end
      t[:name] = 'timer_1sec'
      t = Thread.new do
        loop do
          sleep 10
          timer_event_10sec
        end
      end
      t[:name] = 'timer_10sec'
    end

    def timer_event_1sec
      if @routing_table.enabled_failover
        nodes=@routing_table.nodes
        nodes.delete(@stats.ap_str)
        nodes_check(nodes)
      end

      if (@stats.run_join || @stats.run_recover || @stats.run_balance) &&
          @stats.run_storage_clean_up
        stop_clean_up
      end
    rescue Exception =>e
      @logger.error("#{e}\n#{$@}")
    end

    def timer_event_10sec
      if @startup && @routing_table.enabled_failover == false
        @logger.debug("nodes_check start")
        nodes=@routing_table.nodes
        nodes.delete(@stats.ap_str)
        if nodes_check(nodes)
          @logger.info("all nodes started")
          AsyncProcess::queue.clear
          @routing_table.enabled_failover = true
          Command::Receiver::mk_evlist
          @startup = false
        end
      elsif @routing_table.enabled_failover == false
        @logger.warn("failover disable now!!")
      else
        version_check
        @routing_table.delete_old_trans(@stats.routing_trans_timeout)
        start_sync_routing_process
      end

      if (@routing_table.enabled_failover &&
          @stats.run_storage_clean_up == false &&
          @stats.run_balance == false &&
          @stats.run_recover == false &&
          @stats.run_iterate_storage == false &&
          @stats.run_join == false &&
          @stats.run_receive_a_vnode.empty? &&
          @stats.do_clean_up?)
        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_storage_clean_up_process'))
      end

      if @cr_writer.run_replication
        if @cr_writer.change_mklhash?
          nid = @cr_writer.replica_nodelist.sample
          @cr_writer.update_mklhash(nid)
          @cr_writer.update_nodelist(nid)
          @cr_writer.update_routing_table(nid)
        end
      end

      @stats.clear_counters
    rescue Exception =>e
      @logger.error("#{e}\n#{$@}")
    end

    def nodes_check(nodes)
      nodes.each{|nid|
        return false unless node_check(nid)
      }
      return true
    end

    def node_check(nid)
      if @startup && @routing_table.enabled_failover == false
        unless Roma::Messaging::ConPool.instance.check_connection(nid)
          @logger.info("I'm waiting for booting the #{nid} instance.")
          return false
        end
      end
      name = async_send_cmd(nid,"whoami\r\n",2)
      return false unless name
      if name != @stats.name
        @logger.error("#{nid} has diffarent name.")
        @logger.error("me = \"#{@stats.name}\"  #{nid} = \"#{name}\"")
        return false
      end
      return true
    end

    def version_check
      nodes=@routing_table.nodes
      nodes.each{|nid|
        vs = async_send_cmd(nid,"version\r\n",2)
        next unless vs
        if /VERSION\s(?:ROMA-)?(\d+)\.(\d+)\.(\d+)/ =~ vs
          ver = ($1.to_i << 16) + ($2.to_i << 8) + $3.to_i
          @routing_table.set_version(nid, ver)
        end
      }
    end

    def start_sync_routing_process
      return if @stats.run_join || @stats.run_recover || @stats.run_balance || @stats.run_sync_routing

      nodes = @routing_table.nodes
      return if nodes.length == 1 && nodes[0] == @stats.ap_str

      @stats.run_sync_routing = true

      idx=nodes.index(@stats.ap_str)
      unless idx
        @logger.error("My node-id(=#{@stats.ap_str}) does not found in the routingtable.")
        EventMachine::stop_event_loop
        return
      end
      t = Thread.new{
        begin
          ret = routing_hash_comparison(nodes[idx-1])
          if ret == :inconsistent
            @logger.info("create nodes from v_idx");

            @routing_table.create_nodes_from_v_idx
            begin
              con = Roma::Messaging::ConPool.instance.get_connection(nodes[idx-1])
              con.write("create_nodes_from_v_idx\r\n")
              if con.gets == "CREATED\r\n"
                Roma::Messaging::ConPool.instance.return_connection(nodes[idx-1], con)
              else
                @logger.error("get busy result in create_nodes_from_v_idx command from #{nodes[idx-1]}.")
                con.close
              end
            rescue Exception =>e
              @logger.error("create_nodes_from_v_idx command unreachable to the #{nodes[idx-1]}.")
            end
          end
        rescue Exception =>e
          @logger.error("#{e}\n#{$@}")
        end
        @stats.run_sync_routing = false
      }
      t[:name] = 'sync_routing'
    end

    def routing_hash_comparison(nid,id='0')
      return :skip if @stats.run_join || @stats.run_recover || @stats.run_balance

      h = async_send_cmd(nid,"mklhash #{id}\r\n")
      if h && h.start_with?("ERROR") == false && @routing_table.mtree.get(id) != h
        if (id.length - 1) == @routing_table.div_bits
          sync_routing(nid,id)
        else
          routing_hash_comparison(nid,"#{id}0")
          routing_hash_comparison(nid,"#{id}1")
        end
        return :inconsistent
      end
      :consistent
    end

    def sync_routing(nid,id)
      vn = @routing_table.mtree.to_vn(id)
      @logger.warn("vn=#{vn} inconsistent")

      res = async_send_cmd(nid,"getroute #{vn}\r\n")
      return if res == nil || res.start_with?("ERROR")
      clk,*nids = res.split(' ')
      clk = @routing_table.set_route(vn, clk.to_i, nids)

      if clk.is_a?(Integer) == false
        clk,nids = @routing_table.search_nodes_with_clk(vn)
        cmd = "setroute #{vn} #{clk-1}"
        nids.each{|nid2| cmd << " #{nid2}" }
        async_send_cmd(nid,"#{cmd}\r\n")
      end
    end

    def async_send_cmd(nid, cmd, timeout_sec=nil)
      connection = res = nil
      Timeout.timeout(timeout_sec) do
        connection = Roma::Messaging::ConPool.instance.get_connection(nid)
        unless connection
          @routing_table.proc_failed(nid) if @routing_table
          @logger.error("#{__FILE__}:#{__LINE__}:#{nid} connection refused,command is #{cmd}.")
          return nil
        end
        connection.write(cmd)
        res = connection.gets
      end
      if res == nil
        @routing_table.proc_failed(nid) if @routing_table
        return nil
      elsif res.start_with?("ERROR") == false
        @routing_table.proc_succeed(nid) if @routing_table
        Roma::Messaging::ConPool.instance.return_connection(nid, connection)
      end
      res.chomp
    rescue Exception => e
      @routing_table.proc_failed(nid) if @routing_table
      @logger.error("#{__FILE__}:#{__LINE__}:#{e} #{$@}")
      @logger.error("#{__FILE__}:#{__LINE__}:Send command failed that node-id is #{nid},command is #{cmd}.")
      nil
    end

    def async_broadcast_cmd(cmd,without_nids=nil,tout=nil)
      without_nids=[@stats.ap_str] unless without_nids
      res = {}
      @routing_table.nodes.each{ |nid|
        res[nid] = async_send_cmd(nid,cmd,tout) unless without_nids.include?(nid)
      }
      res
    rescue Exception => e
      @logger.error("#{e}\n#{$@}")
      nil
    end

    def stop
      @storages.each_value do |storage|
        storage.closedb
      end
      if @routing_table.instance_of?(Roma::Routing::ChurnbasedRoutingTable)
        @routing_table.close_log
      end
      @logger.info("Roma server has stopped: #{@stats.ap_str}")
    end

    def validate_version_number_in_config
      # config version check
      if !Config.const_defined?(:VERSION)
        @logger.error("ROMA FAIL TO BOOT! : config.rb's version is too old.")
        exit
      elsif Config::VERSION != Roma::VERSION
        if /(\d+)\.(\d+)\.(\d+)/ =~ Config::VERSION
          version_config = ($1.to_i << 16) + ($2.to_i << 8) + $3.to_i
        end
        if /(\d+)\.(\d+)\.(\d+)/ =~ Roma::VERSION
          version_roma = ($1.to_i << 16) + ($2.to_i << 8) + $3.to_i
        end

        if version_config == version_roma
          @logger.info("This version is development version.")
        else
          @logger.error("ROMA FAIL TO BOOT! : config.rb's version is differ from current ROMA version.")
          exit
        end
      end
    end

    def daemonize
      Process.daemon(true, true)
    end

    def check_pid!
      return unless File.exist?(pid_file_path)
      begin
        pid = File.read(pid_file_path).to_i
        File.delete(pid_file_path) if pid.zero?

        $stderr.puts "A server is already running. Check #{pid_file_path}"
        exit(1)
      rescue Errno::ESRCH
        File.delete(pid_file_path)
      rescue Errno::EPERM
        $stderr.puts "A server is already running. Check #{pid_file_path}"
        exit(1)
      end
    end

    def write_pid
      File.open(pid_file_path, 'w') { |f| f.write(Process.pid.to_s) }
      at_exit { FileUtils.rm_f(pid_file_path) }
    rescue Errno::EEXIST
      check_pid!
      retry
    end

    def pid_file_path
      File.expand_path("./#{@stats.address}_#{@stats.port}.pid", DEFAULT_PID_DIR)
    end

  end # class Romad
end # module Roma

