
module Roma
  module Command

    module SystemCommandReceiver

      # balse [reason]
      def ev_balse(s)
        send_data("Are you sure?(yes/no)\r\n")
        if gets != "yes\r\n"
          close_connection_after_writing
          return
        end

        if s.length == 2
          @log.info("Receive a balse #{s[1]}")
        else
          @log.info("Receive a balse command.")
        end
        @rttable.enabled_failover = false
        res = broadcast_cmd("rbalse\r\n")
        send_data("#{res.inspect}\r\n")
        close_connection_after_writing
        @stop_event_loop = true
      end

      # rbalse [reason]
      def ev_rbalse(s)
        if s.length == 2
          @log.info("Receive a rbalse #{s[1]}")
        else
          @log.info("Receive a rbalse command.")
        end
        @rttable.enabled_failover = false
        send_data("BYE\r\n")
        close_connection_after_writing
        @stop_event_loop = true
      end

      # version
      def ev_version(s)
        send_data("VERSION ROMA-#{Roma::VERSION}\r\n")
      end

      # quit
      def ev_quit(s)
        close_connection
      end

      def ev_whoami(s)
        send_data("#{@stats.name}\r\n")
      end

      # stats [regexp]
      def ev_stats(s); ev_stat(s); end

      # stat [regexp]
      def ev_stat(s)
        regexp = s[1] if s.length == 2
        h = {}
        h['version'] = Roma::VERSION
        send_stat_result(nil,h,regexp)
        send_stat_result(nil,Roma::Config.get_stat,regexp)
        send_stat_result(nil,@stats.get_stat,regexp)
        @storages.each{|hname,st|
          send_stat_result("storages[#{hname}].",st.get_stat,regexp)
        }
        send_stat_result(nil,$roma.wb_get_stat,regexp)
        send_stat_result(nil,@rttable.get_stat(@stats.ap_str),regexp)
        send_stat_result(nil,conn_get_stat,regexp)
        send_data("END\r\n")
      end

      def send_stat_result(prefix,h,regexp = nil)
        h.each{|k,v|         
          if prefix
            key = "#{prefix}#{k}"
          else
            key = "#{k}"
          end
          if regexp
            send_data("#{key} #{v}\r\n") if key =~ /#{regexp}/
          else
            send_data("#{key} #{v}\r\n")
          end
        }
      end
      private :send_stat_result

      # writebehind_rotate [hash_name]
      def ev_writebehind_rotate(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        res = broadcast_cmd("rwritebehind_rotate #{s[1]}\r\n")

        if $roma.wb_rotate(s[1])
          res[@stats.ap_str] = "ROTATED"
        else
          res[@stats.ap_str] = "NOT_OPEND"
        end
        send_data("#{res}\r\n")
      end

      # rwritebehind_rotate [hash_name]
      def ev_rwritebehind_rotate(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        if $roma.wb_rotate(s[1])
          send_data("ROTATED\r\n")
        else
          send_data("NOT_OPEND\r\n")
        end
      end

      # writebehind_get_path [hash_name]
      def ev_writebehind_get_path(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        res = broadcast_cmd("rwritebehind_get_path #{s[1]}\r\n")
        
        ret = $roma.wb_get_path(s[1])
        res[@stats.ap_str] = ret

        send_data("#{res}\r\n")
      end

      # rwritebehind_get_path [hash_name]
      def ev_rwritebehind_get_path(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        
        ret = $roma.wb_get_path(s[1])
        send_data("#{ret}\r\n")
      end
      
      # writebehind_get_current_file [hash_name]
      def ev_writebehind_get_current_file(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        res = broadcast_cmd("rwritebehind_get_current_file #{s[1]}\r\n")

        ret = $roma.wb_get_current_file_path(s[1])
        if ret
          res[@stats.ap_str] = ret
        else
          res[@stats.ap_str] = "NOT_OPEND"
        end
        send_data("#{res}\r\n")
      end

      # rwritebehind_get_current_file [hash_name]
      def ev_rwritebehind_get_current_file(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        ret = $roma.wb_get_current_file_path(s[1])
        if ret
          send_data("#{ret}\r\n")
        else
          send_data("NOT_OPEND\r\n")
        end
      end

      # dcnice command is setting priority for a data-copy thread.
      # a niceness of 1 is the highest priority and 5 is the lowest priority.
      # dcnice <priority:1 to 5>
      def ev_dcnice(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        res = broadcast_cmd("rdcnice #{s[1]}\r\n")
        res[@stats.ap_str] = dcnice(s[1].to_i)
        send_data("#{res}\r\n")
      end

      def ev_rdcnice(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end

        send_data("#{dcnice(s[1].to_i)}\r\n")
      end
      
      def ev_restart(s)
        res = broadcast_cmd("rrestart\r\n")
        $roma.eventloop = true
        @rttable.enabled_failover = false
        Messaging::ConPool.instance.close_all
        Event::EMConPool::instance.close_all
        EventMachine::stop_event_loop
        res[@stats.ap_str] = "RESTARTED"
        send_data("#{res}\r\n")
      end

      def ev_rrestart(s)
        $roma.eventloop = true
        @rttable.enabled_failover = false
        Messaging::ConPool.instance.close_all
        Event::EMConPool::instance.close_all
        EventMachine::stop_event_loop
        send_data("RESTARTED\r\n")
      end

      # set_log_level [ 'debug' | 'info' | 'warn' | 'error' ] 
      def ev_set_log_level(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end

        case s[1].downcase
        when 'debug'
          @log.level = Roma::Logging::RLogger::Severity::DEBUG
        when 'info'
          @log.level = Roma::Logging::RLogger::Severity::INFO
        when 'warn'
          @log.level = Roma::Logging::RLogger::Severity::WARN
        when 'error'
          @log.level = Roma::Logging::RLogger::Severity::ERROR
        else
          return send_data("CLIENT_ERROR no match log-level string\r\n")
        end
        send_data("STORED\r\n")
      end

      # out <key> <vn>
      def ev_out(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        if s.length >= 3
          vn = s[2].to_i
        else
          d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
          vn = @rttable.get_vnode_id(d)
        end
        res = @storages[hname].out(vn, key, 0)
        @stats.out_message_count += 1
        unless res
          return send_data("NOT_DELETED\r\n")
        end
        send_data("DELETED\r\n")
      end

      # rset <key> <hash value> <timelimit> <length>
      # "set" means "store this data".
      # <command name> <key> <digest> <exptime> <bytes> [noreply]\r\n
      # <data block>\r\n
      def ev_rset(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = s[2].to_i
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits if d == 0
        data = read_bytes(s[5].to_i)
        read_bytes(2)
        vn = @rttable.get_vnode_id(d)
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end
        if @storages[hname].rset(vn, key, d, s[3].to_i, s[4].to_i, data)
          send_data("STORED\r\n")
        else
          @log.error("rset NOT_STORED:#{@storages[hname].error_message} #{vn} #{s[1]} #{d} #{s[3]} #{s[4]}")
          send_data("NOT_STORED\r\n")
        end
        @stats.redundant_count += 1
      end

      # <command name> <key> <digest> <exptime> <bytes> [noreply]\r\n
      # <compressed data block>\r\n
      def ev_rzset(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = s[2].to_i
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits if d == 0
        zdata = read_bytes(s[5].to_i)
        read_bytes(2)
        vn = @rttable.get_vnode_id(d)
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end

        data = Zlib::Inflate.inflate(zdata)
# @log.debug("data = #{data}")
        if @storages[hname].rset(vn, key, d, s[3].to_i, s[4].to_i, data)
          send_data("STORED\r\n")
        else
          @log.error("rzset NOT_STORED:#{@storages[hname].error_message} #{vn} #{s[1]} #{d} #{s[3]} #{s[4]}")
          send_data("NOT_STORED\r\n")
        end
        @stats.redundant_count += 1
      rescue Zlib::DataError => e
        @log.error("rzset NOT_STORED:#{e} #{vn} #{s[1]} #{d} #{s[3]} #{s[4]}")
        send_data("NOT_STORED\r\n")
      end

      def ev_forcedly_start(s)
        @log.info("ROMA forcedly start.")
        AsyncProcess::queue.clear
        @rttable.enabled_failover = true
        Command::Receiver::mk_evlist
        $roma.startup = false
        send_data("STARTED\r\n")
      end

      # switch_failover <on|off>
      def ev_switch_failover(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        res = broadcast_cmd("rswitch_failover #{s[1]}\r\n")
        if s[1] == 'on'
          Messaging::ConPool.instance.close_all
          Event::EMConPool::instance.close_all
          @rttable.enabled_failover = true
          @log.info("failover enabled")
          res[@stats.ap_str] = "ENABLED"
        elsif s[1] == 'off'
          @rttable.enabled_failover = false
          @log.info("failover disabled")
          res[@stats.ap_str] = "DISABLED"
        else
          res[@stats.ap_str] = "NOTSWITCHED"
        end
        send_data("#{res}\r\n")
      end

      # rswitch_failover <on|off>
      def ev_rswitch_failover(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        if s[1] == 'on'
          Messaging::ConPool.instance.close_all
          Event::EMConPool::instance.close_all
          @rttable.enabled_failover = true
          @log.info("failover enabled")
          return send_data("ENABLED\r\n")
        elsif s[1] == 'off'
          @rttable.enabled_failover = false
          @log.info("failover disabled")
          return send_data("DISABLED\r\n")
        else
          send_data("NOTSWITCHED\r\n")
        end
      end

      def ev_set_continuous_limit(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end

        res = broadcast_cmd("rset_continuous_limit #{s[1]}\r\n")

        if Event::Handler.set_ccl(s[1])
          res[@stats.ap_str] = "STORED"
        else
          res[@stats.ap_str] = "NOT_STORED"
        end
        send_data("#{res}\r\n")
      end

      def ev_rset_continuous_limit(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        if Event::Handler.set_ccl(s[1])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      end

      # set_connection_pool_maxlength <length>
      # set to max length of the connection pool
      def ev_set_connection_pool_maxlength(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        if s[1].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end

        res = broadcast_cmd("rset_connection_pool_maxlength #{s[1]}\r\n")
        Messaging::ConPool.instance.maxlength = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_connection_pool_maxlength <length>
      def ev_rset_connection_pool_maxlength(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        if s[1].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end

        Messaging::ConPool.instance.maxlength = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_connection_pool_maxlength <length>
      # set to max length of the connection pool
      def ev_set_emconnection_pool_maxlength(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        if s[1].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end

        res = broadcast_cmd("rset_emconnection_pool_maxlength #{s[1]}\r\n")
        Event::EMConPool.instance.maxlength = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_connection_pool_maxlength <length>
      def ev_rset_emconnection_pool_maxlength(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        if s[1].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end

        Event::EMConPool.instance.maxlength = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_accepted_connection_expire_time <sec>
      # set to expired time(sec) for accepted connections
      def ev_set_accepted_connection_expire_time(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end

        res = broadcast_cmd("rset_accepted_connection_expire_time #{s[1]}\r\n")
        Event::Handler::connection_expire_time = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_accepted_connection_expire_time <sec>
      def ev_rset_accepted_connection_expire_time(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        Event::Handler::connection_expire_time = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_hilatency_warn_time <sec>
      # set to threshold of warn message into a log when hilatency occured in a command.
      def ev_set_hilatency_warn_time(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        if s[1].to_f <= 0
          return send_data("CLIENT_ERROR time value must be lager than 0\r\n")
        end

        res = broadcast_cmd("rset_hilatency_warn_time #{s[1]}\r\n")
        @stats.hilatency_warn_time = s[1].to_f
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_hilatency_warn_time <sec>
      def ev_rset_hilatency_warn_time(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        if s[1].to_f <= 0
          return send_data("CLIENT_ERROR time value must be lager than 0\r\n")
        end
        @stats.hilatency_warn_time = s[1].to_f
        send_data("STORED\r\n")
      end

      private 

      def dcnice(p)
        case(p)
        when 1 # highest priority
          @stats.stream_copy_wait_param = 0.001
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.001
            st.each_vn_dump_sleep_count = 1000
          }
        when 2
          @stats.stream_copy_wait_param = 0.005
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.005
            st.each_vn_dump_sleep_count = 100
          }
        when 3 # default priority
          @stats.stream_copy_wait_param = 0.01
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.001
            st.each_vn_dump_sleep_count = 10
          }
        when 4
          @stats.stream_copy_wait_param = 0.01
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.005
            st.each_vn_dump_sleep_count = 10
          }
        when 5 # lowest priority
          @stats.stream_copy_wait_param = 0.01
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.01
            st.each_vn_dump_sleep_count = 10
          }
        else
          return "CLIENT_ERROR You sholud input a priority from 1 to 5."
        end
        "STORED"
      end
    end # module SystemCommandReceiver

  end # module Command
end # module Roma
