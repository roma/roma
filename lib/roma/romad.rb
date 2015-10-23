#!/usr/bin/env ruby
require 'optparse'
require 'roma/version'
require 'roma/stats'
require 'roma/command_plugin'
require 'roma/async_process'
require 'roma/write_behind'
require 'roma/logging/rlogger'
require 'roma/command/receiver'
require 'roma/messaging/con_pool'
require 'roma/event/con_pool'
require 'roma/routing/cb_rttable'
require 'timeout'

module Roma

  class Romad
    include AsyncProcess
    include WriteBehindProcess

    attr :storages
    attr :rttable
    attr :stats
    attr :wb_writer
    attr :cr_writer

    attr_accessor :eventloop
    attr_accessor :startup

    def initialize(argv = nil)
      @stats = Roma::Stats.instance
      @startup = true
      options(argv)
      initialize_stats
      initialize_connection
      initialize_logger
      initialize_rttable
      initialize_storages
      initialize_handler
      initialize_plugin
      initialize_wb_writer
    end

    def start
      # config version check
      if !Config.const_defined?(:VERSION)
        @log.error("ROMA FAIL TO BOOT! : config.rb's version is too old.")
        exit
      elsif Config::VERSION != Roma::VERSION
        if /(\d+)\.(\d+)\.(\d+)/ =~ Config::VERSION
          version_config = ($1.to_i << 16) + ($2.to_i << 8) + $3.to_i
        end
        if /(\d+)\.(\d+)\.(\d+)/ =~ Roma::VERSION
          version_roma = ($1.to_i << 16) + ($2.to_i << 8) + $3.to_i
        end

        if version_config == version_roma
          @log.info("This version is development version.")
        else 
          @log.error("ROMA FAIL TO BOOT! : config.rb's version is differ from current ROMA version.")
          exit
        end
      end

      if node_check(@stats.ap_str)
        @log.error("#{@stats.ap_str} is already running.")
        return
      end

      @storages.each{|hashname,st|
        st.opendb
      }

      start_async_process
      start_wb_process
      timer

      if @stats.join_ap
        AsyncProcess::queue.push(AsyncMessage.new('start_join_process'))
      end

      # select a kind of system call
      if Config.const_defined?(:CONNECTION_USE_EPOLL) && Config::CONNECTION_USE_EPOLL
        @log.info("use an epoll")
        EM.epoll
        if Config.const_defined?(:CONNECTION_DESCRIPTOR_TABLE_SIZE)
          EM.set_descriptor_table_size(Config::CONNECTION_DESCRIPTOR_TABLE_SIZE)
        end
      else
        @log.info("use a select")
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
              @log.error("#{e}\n#{$@}")
            end
          }
          Event::Handler::connections.clear

          EventMachine::run do
            EventMachine.start_server('0.0.0.0', @stats.port,
                                      Roma::Command::Receiver,
                                      @storages, @rttable)
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
                        @log.info("connection expired from #{k.addr}:#{k.port},lastcmd = #{k.lastcmd}")
                      else
                        @log.info("connection expired in irregular connection")
                        dellist << k
                      end
                    rescue Exception => e
                      @log.error("#{e}\n#{$@}")
                      dellist << k
                    end
                  end
                }
                dellist.each{|k|
                  @log.info("delete connection lastcmd = #{k.lastcmd}")
                  Event::Handler::connections.delete(k)
                }
              end
            }

            @log.info("Now accepting connections on address #{@stats.address}, port #{@stats.port}")
          end
        rescue Interrupt => e
          if daemon?
            @log.error("#{e.inspect}\n#{$@}")
            retry
          else
            $stderr.puts "#{e.inspect}"
          end
        rescue Exception => e
          @log.error("#{e}\n#{$@}")
          @log.error("restart an eventmachine")
          retry
        end
      end
      stop_async_process
      stop_wb_process
      stop
    end

    def daemon?; @stats.daemon; end

    def stop_clean_up
      @stats.last_clean_up = Time.now
      while(@stats.run_storage_clean_up)
        @log.info("Storage clean up process will be stop.")
        @storages.each_value{|st| st.stop_clean_up}
        sleep 0.005
      end
    end

    private

    def initialize_stats
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
                                                     @log)

      @cr_writer = Roma::WriteBehind::StreamWriter.new(@log)
    end

    def initialize_plugin
      return unless Roma::Config.const_defined? :PLUGIN_FILES

      Roma::Config::PLUGIN_FILES.each do|f|
        require "roma/plugin/#{f}"
        @log.info("roma/plugin/#{f} loaded")
      end
      Roma::CommandPlugin.plugins.each do|plugin|
          Roma::Command::Receiver.class_eval do
            include plugin
          end
          @log.info("#{plugin.to_s} included")
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
            @log.info("command log:#{ret.chomp}") if ret
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
      @log = Roma::Logging::RLogger.instance

      if Config.const_defined? :LOG_LEVEL
        case Config::LOG_LEVEL
        when :debug
          @log.level = Roma::Logging::RLogger::Severity::DEBUG
        when :info
          @log.level = Roma::Logging::RLogger::Severity::INFO
        when :warn
          @log.level = Roma::Logging::RLogger::Severity::WARN
        when :error
          @log.level = Roma::Logging::RLogger::Severity::ERROR
        end
      end
    end

    def options(argv)
      opts = OptionParser.new
      opts.banner="usage:#{File.basename($0)} [options] address"

      @stats.daemon = false
      opts.on("-d","--daemon") { |v| @stats.daemon = true }

      opts.on_tail("-h", "--help", "Show this message") {
        puts opts; exit
      }

      opts.on("-j","--join [address:port]") { |v| @stats.join_ap = v }

      opts.on("-p", "--port [PORT]") { |v| @stats.port = v }

      @stats.verbose = false
      opts.on(nil,"--verbose"){ |v| @stats.verbose = true }

      opts.on_tail("-v", "--version", "Show version") {
        puts "romad.rb #{Roma::VERSION}"; exit
      }

      opts.on("-n", "--name [name]") { |v| @stats.name = v }

      ##
      # "--enabled_repeathost" is deplicated. We will rename it to "--replication_in_host"
      ##
      @stats.enabled_repetition_host_in_routing = false
      opts.on(nil,"--enabled_repeathost", "Allow redundancy to same host"){
        @stats.enabled_repetition_host_in_routing = true
        puts "Warning: \"--enabled_repeathost\" is deplicated. Please use \"--replication_in_host\""
      }
      opts.on(nil,"--replication_in_host", "Allow redundancy to same host"){
        @stats.enabled_repetition_host_in_routing = true
      }

      @stats.disabled_cmd_protect = false
      opts.on(nil,"--disabled_cmd_protect", "Command protection disable while starting"){
        @stats.disabled_cmd_protect = true
      }

      opts.on("--config [file path of the config.rb]"){ |v| @stats.config_path = File.expand_path(v) }

      opts.parse!(argv)
      raise OptionParser::ParseError.new if argv.length < 1
      @stats.address = argv[0]

      @stats.config_path = 'roma/config' unless @stats.config_path

      unless require @stats.config_path
        raise "config.rb has already been load outside the romad.rb."
      end

      @stats.name = Config::DEFAULT_NAME unless @stats.name
      @stats.port = Config::DEFAULT_PORT.to_s unless @stats.port

      unless @stats.port =~ /^\d+$/
        raise OptionParser::ParseError.new('Port number is not numeric.')
      end

      @stats.join_ap.sub!(':','_') if @stats.join_ap
      if @stats.join_ap && !(@stats.join_ap =~ /^.+_\d+$/)
        raise OptionParser::ParseError.new('[address:port] can not parse.')
      end
    rescue OptionParser::ParseError => e
      $stderr.puts e.message
      $stderr.puts opts.help
      exit 1
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
      Dir.glob("#{path}/*").each{|f|
        if File.directory?(f)
          hname = File.basename(f)
          st = st_class.new
          st.storage_path = "#{path}/#{hname}"
          st.vn_list = @rttable.vnodes
          st.st_class = st_class
          st.divnum = st_divnum
          st.option = st_option
          @storages[hname] = st
        end
      }
      if @storages.length == 0
        hname = 'roma'
        st = st_class.new
        st.storage_path = "#{path}/#{hname}"
        st.vn_list = @rttable.vnodes
        st.st_class = st_class
        st.divnum = st_divnum
        st.option = st_option
        @storages[hname] = st
      end
    end

    def initialize_rttable
      if @stats.join_ap
        initialize_rttable_join
      else
        fname = "#{Roma::Config::RTTABLE_PATH}/#{@stats.ap_str}.route"
        raise "#{fname} not found." unless File::exist?(fname)
        rd = Roma::Routing::RoutingData::load(fname)
        raise "It failed in loading the routing table data." unless rd
        if Config.const_defined? :RTTABLE_CLASS
          @rttable = Config::RTTABLE_CLASS.new(rd,fname)
        else
          @rttable = Roma::Routing::ChurnbasedRoutingTable.new(rd,fname)
        end
      end

      if Roma::Config.const_defined?(:RTTABLE_SUB_NID)
        @rttable.sub_nid = Roma::Config::RTTABLE_SUB_NID
      end

      if Roma::Config.const_defined?(:ROUTING_FAIL_CNT_THRESHOLD)
        @rttable.fail_cnt_threshold = Roma::Config::ROUTING_FAIL_CNT_THRESHOLD
      end
      if Roma::Config.const_defined?(:ROUTING_FAIL_CNT_GAP)
        @rttable.fail_cnt_gap = Roma::Config::ROUTING_FAIL_CNT_GAP
      end
      @rttable.lost_action = Roma::Config::DEFAULT_LOST_ACTION
      @rttable.auto_recover = Roma::Config::AUTO_RECOVER if defined?(Roma::Config::AUTO_RECOVER)

      @rttable.enabled_failover = false
      @rttable.set_leave_proc{|nid|
        Roma::Messaging::ConPool.instance.close_same_host(nid)
        Roma::Event::EMConPool.instance.close_same_host(nid)
        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('broadcast_cmd',["leave #{nid}",[@stats.ap_str,nid,5]]))
      }
      @rttable.set_lost_proc{
        if @rttable.lost_action == :shutdown
          async_broadcast_cmd("rbalse lose_data\r\n")
          EventMachine::stop_event_loop
          @log.error("Romad has stopped, so that lose data.")
        end
      }
      @rttable.set_recover_proc{|action|
        if (@rttable.lost_action == :shutdown || @rttable.lost_action == :auto_assign) && @rttable.auto_recover == true
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new("#{action}"))
        elsif
          @log.error("AUTO_RECOVER is off or Unavailable value is set to [DEFAULT_LOST_ACTION] => #{@rttable.lost_action}")
        end
      }

      if Roma::Config.const_defined?(:ROUTING_EVENT_LIMIT_LINE)
        @rttable.event_limit_line = Roma::Config::ROUTING_EVENT_LIMIT_LINE
      end
      Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_get_routing_event'))
    end

    def initialize_rttable_join
      name = async_send_cmd(@stats.join_ap,"whoami\r\n")
      unless name
        raise "No respons from #{@stats.join_ap}."
      end

      if name != @stats.name
        raise "#{@stats.join_ap} has diffarent name.\n" +
          "me = \"#{@stats.name}\"  #{@stats.join_ap} = \"#{name}\""
      end

      fname = "#{Roma::Config::RTTABLE_PATH}/#{@stats.ap_str}.route"
      if rd = get_routedump(@stats.join_ap)
        rd.save(fname)
      else
        raise "It failed in getting the routing table data from #{@stats.join_ap}."
      end

      if rd.nodes.include?(@stats.ap_str)
        raise "ROMA has already contained #{@stats.ap_str}."
      end

      @rttable = Roma::Routing::ChurnbasedRoutingTable.new(rd,fname)
      nodes = @rttable.nodes

      nodes.each{|nid|
        begin
          con = Roma::Messaging::ConPool.instance.get_connection(nid)
          con.write("join #{@stats.ap_str}\r\n")
          if con.gets != "ADDED\r\n"
            raise "Hotscale initialize failed.\n#{nid} is busy."
          end
          Roma::Messaging::ConPool.instance.return_connection(nid, con)
        rescue =>e
          raise "Hotscale initialize failed.\n#{nid} unreachable connection."
        end
      }
      @rttable.add_node(@stats.ap_str)
    end

    def get_routedump(nid)
      rcv = receive_routing_dump(nid, "routingdump bin\r\n")
      unless rcv
        rcv = receive_routing_dump(nid, "routingdump\r\n")
        rd = Marshal.load(rcv)
      else
        rd = Routing::RoutingData.decode_binary(rcv)
      end
      rd
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
      if @rttable.enabled_failover
        nodes=@rttable.nodes
        nodes.delete(@stats.ap_str)
        nodes_check(nodes)
      end

      if (@stats.run_join || @stats.run_recover || @stats.run_balance) &&
          @stats.run_storage_clean_up
        stop_clean_up
      end
    rescue Exception =>e
      @log.error("#{e}\n#{$@}")
    end

    def timer_event_10sec
      if @startup && @rttable.enabled_failover == false
        @log.debug("nodes_check start")
        nodes=@rttable.nodes
        nodes.delete(@stats.ap_str)
        if nodes_check(nodes)
          @log.info("all nodes started")
          AsyncProcess::queue.clear
          @rttable.enabled_failover = true
          Command::Receiver::mk_evlist
          @startup = false
        end
      elsif @rttable.enabled_failover == false
        @log.warn("failover disable now!!")
      else
        version_check
        @rttable.delete_old_trans(@stats.routing_trans_timeout)
        start_sync_routing_process
      end

      if (@rttable.enabled_failover &&
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
          @cr_writer.update_rttable(nid)
        end
      end

      @stats.clear_counters
    rescue Exception =>e
      @log.error("#{e}\n#{$@}")
    end

    def nodes_check(nodes)
      nodes.each{|nid|
        return false unless node_check(nid)
      }
      return true
    end

    def node_check(nid)
      if @startup && @rttable.enabled_failover == false
        unless Roma::Messaging::ConPool.instance.check_connection(nid) 
          @log.info("I'm wating for booting the #{nid} instance.")
          return false
        end
      end
      name = async_send_cmd(nid,"whoami\r\n",2)
      return false unless name
      if name != @stats.name
        @log.error("#{nid} has diffarent name.")
        @log.error("me = \"#{@stats.name}\"  #{nid} = \"#{name}\"")
        return false
      end
      return true
    end

    def version_check
      nodes=@rttable.nodes
      nodes.each{|nid|
        vs = async_send_cmd(nid,"version\r\n",2)
        next unless vs
        if /VERSION\s(?:ROMA-)?(\d+)\.(\d+)\.(\d+)/ =~ vs
          ver = ($1.to_i << 16) + ($2.to_i << 8) + $3.to_i
          @rttable.set_version(nid, ver)
        end
      }
    end

    def start_sync_routing_process
      return if @stats.run_join || @stats.run_recover || @stats.run_balance || @stats.run_sync_routing

      nodes = @rttable.nodes
      return if nodes.length == 1 && nodes[0] == @stats.ap_str

      @stats.run_sync_routing = true

      idx=nodes.index(@stats.ap_str)
      unless idx
        @log.error("My node-id(=#{@stats.ap_str}) does not found in the routingtable.")
        EventMachine::stop_event_loop
        return
      end
      t = Thread.new{
        begin
          ret = routing_hash_comparison(nodes[idx-1])
          if ret == :inconsistent
            @log.info("create nodes from v_idx");

            @rttable.create_nodes_from_v_idx
            begin
              con = Roma::Messaging::ConPool.instance.get_connection(nodes[idx-1])
              con.write("create_nodes_from_v_idx\r\n")
              if con.gets == "CREATED\r\n"
                Roma::Messaging::ConPool.instance.return_connection(nodes[idx-1], con)
              else
                @log.error("get busy result in create_nodes_from_v_idx command from #{nodes[idx-1]}.")
                con.close
              end
            rescue Exception =>e
              @log.error("create_nodes_from_v_idx command unreachable to the #{nodes[idx-1]}.")
            end
          end
        rescue Exception =>e
          @log.error("#{e}\n#{$@}")
        end
        @stats.run_sync_routing = false
      }
      t[:name] = 'sync_routing'
    end

    def routing_hash_comparison(nid,id='0')
      return :skip if @stats.run_join || @stats.run_recover || @stats.run_balance

      h = async_send_cmd(nid,"mklhash #{id}\r\n")
      if h && h.start_with?("ERROR") == false && @rttable.mtree.get(id) != h
        if (id.length - 1) == @rttable.div_bits
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
      vn = @rttable.mtree.to_vn(id)
      @log.warn("vn=#{vn} inconsistent")

      res = async_send_cmd(nid,"getroute #{vn}\r\n")
      return if res == nil || res.start_with?("ERROR")
      clk,*nids = res.split(' ')
      clk = @rttable.set_route(vn, clk.to_i, nids)

      if clk.is_a?(Integer) == false
        clk,nids = @rttable.search_nodes_with_clk(vn)
        cmd = "setroute #{vn} #{clk-1}"
        nids.each{|nid2| cmd << " #{nid2}" }
        async_send_cmd(nid,"#{cmd}\r\n")
      end
    end

    def async_send_cmd(nid, cmd, tout=nil)
      con = res = nil
      if tout
        timeout(tout){
          con = Roma::Messaging::ConPool.instance.get_connection(nid)
          unless con
            @rttable.proc_failed(nid) if @rttable
            @log.error("#{__FILE__}:#{__LINE__}:#{nid} connection refused,command is #{cmd}.")
            return nil
          end
          con.write(cmd)
          res = con.gets
        }
      else
        con = Roma::Messaging::ConPool.instance.get_connection(nid)
        unless con
          @rttable.proc_failed(nid) if @rttable
          @log.error("#{__FILE__}:#{__LINE__}:#{nid} connection refused,command is #{cmd}.")
          return nil
        end
        con.write(cmd)
        res = con.gets
      end
      if res == nil
        @rttable.proc_failed(nid) if @rttable
        return nil
      elsif res.start_with?("ERROR") == false
        @rttable.proc_succeed(nid) if @rttable
        Roma::Messaging::ConPool.instance.return_connection(nid, con)
      end
      res.chomp
    rescue Exception => e
      @rttable.proc_failed(nid) if @rttable
      @log.error("#{__FILE__}:#{__LINE__}:#{e} #{$@}")
      @log.error("#{__FILE__}:#{__LINE__}:Send command failed that node-id is #{nid},command is #{cmd}.")
      nil
    end

    def async_broadcast_cmd(cmd,without_nids=nil,tout=nil)
      without_nids=[@stats.ap_str] unless without_nids
      res = {}
      @rttable.nodes.each{ |nid|
        res[nid] = async_send_cmd(nid,cmd,tout) unless without_nids.include?(nid)
      }
      res
    rescue Exception => e
      @log.error("#{e}\n#{$@}")
      nil
    end

    def stop
      @storages.each_value{|st|
        st.closedb
      }
      if @rttable.instance_of?(Roma::Routing::ChurnbasedRoutingTable)
        @rttable.close_log
      end
      @log.info("Romad has stopped: #{@stats.ap_str}")
    end

  end # class Romad
end # module Roma

