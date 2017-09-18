require 'roma/async_process'

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
        res[@stats.ap_str] = "BYE"
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

      # shutdown [reason]
      def ev_shutdown(s)
        send_data("*** ARE YOU REALLY SURE TO SHUTDOWN? *** (yes/no)\r\n")
        if gets != "yes\r\n"
          close_connection_after_writing
          return
        end

        if s.length == 2
          @log.info("Receive a shutdown #{s[1]}")
        else
          @log.info("Receive a shutdown command.")
        end
        @rttable.enabled_failover = false
        res = broadcast_cmd("rshutdown\r\n")
        res[@stats.ap_str] = "BYE"
        send_data("#{res.inspect}\r\n")
        close_connection_after_writing
        @stop_event_loop = true
      end

      # rshutdown [reason]
      def ev_rshutdown(s)
        if s.length == 2
          @log.info("Receive a rshutdown #{s[1]}")
        else
          @log.info("Receive a rshutdown command.")
        end
        @rttable.enabled_failover = false
        send_data("BYE\r\n")
        close_connection_after_writing
        @stop_event_loop = true
      end

      # shutdown_self
      def ev_shutdown_self(s)
        if s.length != 1
          send_data("ERROR: shutdown_instance has irregular argument.\r\n")
        else
          send_data("\r\n=================================================================\r\n")
          send_data("CAUTION!!: \r\n\tThis command kill the instance!\r\n\tThere is some possibility of occuring redundancy down!\r\n")
          send_data("=================================================================\r\n")
          send_data("\r\nAre you sure to shutdown this instance?(yes/no)\r\n")
          if gets != "yes\r\n"
            close_connection_after_writing
            return
          end
          @log.info("Receive a shutdown_self command.")
          @rttable.enabled_failover = false
          send_data("BYE\r\n")
          @stop_event_loop = true
          close_connection_after_writing
        end
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
        send_stat_result(nil,get_config_stat,regexp)
        send_stat_result(nil,@stats.get_stat,regexp)
        @storages.each{|hname,st|
          send_stat_result("storages[#{hname}].",st.get_stat,regexp)
        }
        send_stat_result(nil,$roma.wb_get_stat,regexp)
        send_stat_result(nil,@rttable.get_stat(@stats.ap_str),regexp)
        send_stat_result(nil,conn_get_stat,regexp)
        send_stat_result(nil,DNSCache.instance.get_stat,regexp)
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

      # switch_replication command is change status of cluster replication
      # if you want to activate, assign 1 nid(addr_port) of replication cluster as argument.
      # if you want to copy existing data, add the 'all' after nid as argument
      # switch_replication <true|false> [nid] [copy target]
      def ev_switch_replication(s)
        unless s.length.between?(2, 4)
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        unless s[1] =~ /^(true|false)$/
          return send_data("CLIENT_ERROR value must be true or false\r\n")
        end
        if s[3] && s[3] != 'all'
          return send_data("CLIENT_ERROR [copy target] must be all or nil\r\n")
        end

        res = broadcast_cmd("rswitch_replication #{s[1]} #{s[2]} #{s[3]}\r\n")

       Timeout.timeout(1){
          case s[1]
          when 'true'
            $roma.cr_writer.update_mklhash(s[2])
            $roma.cr_writer.update_nodelist(s[2])
            $roma.cr_writer.update_rttable(s[2])
            $roma.cr_writer.run_replication = true
            if s[3] == 'all'
              $roma.cr_writer.run_existing_data_replication = true
              Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_replicate_existing_data_process', [$roma.cr_writer.replica_rttable]))
            end
            res[@stats.ap_str] = "ACTIVATED"
          when 'false'
            $roma.cr_writer.replica_mklhash = nil
            $roma.cr_writer.replica_nodelist = []
            $roma.cr_writer.replica_rttable = nil
            $roma.cr_writer.run_replication = false
            $roma.cr_writer.run_existing_data_replication = false
            res[@stats.ap_str] = "DEACTIVATED"
          end
        }
        send_data("#{res}\r\n")
      rescue => e
        send_data("#{e.class}: #{e}\r\n")
      end

      # rswitch_replication <true|false> [nid] [copy target]
      def ev_rswitch_replication(s)
        unless s.length.between?(2, 4)
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        unless s[1] =~ /^(true|false)$/
          return send_data("CLIENT_ERROR value must be true or false\n\r")
        end
        if s[3] && s[3] != 'all'
          return send_data("CLIENT_ERROR [copy target] must be all or nil\r\n")
        end

       Timeout.timeout(1){
          case s[1]
          when 'true'
            $roma.cr_writer.update_mklhash(s[2])
            $roma.cr_writer.update_nodelist(s[2])
            $roma.cr_writer.update_rttable(s[2])
            $roma.cr_writer.run_replication = true
            if s[3] == 'all'
              $roma.cr_writer.run_existing_data_replication = true
              Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_replicate_existing_data_process', [$roma.cr_writer.replica_rttable]))
            end
            send_data("ACTIVATED\r\n")
          when 'false'
            $roma.cr_writer.replica_mklhash = nil
            $roma.cr_writer.replica_nodelist = []
            $roma.cr_writer.replica_rttable = nil
            $roma.cr_writer.run_replication = false
            $roma.cr_writer.run_existing_data_replication = false
            send_data("DEACTIVATED\r\n")
          end
        }
      rescue => e
        send_data("#{e.class}: #{e}\r\n")
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

        @stats.log_level = s[1].downcase

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
          send_data("SERVER_ERROR #{hname} does not exists.\r\n")
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
          send_data("SERVER_ERROR #{hname} does not exists.\r\n")
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

      def ev_set_descriptor_table_size(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif s[1].to_i < 1024
          return send_data("CLIENT_ERROR length must be greater than 1024\r\n")
        end

        res = broadcast_cmd("rset_descriptor_table_size #{s[1]}\r\n")

        EM.set_descriptor_table_size(s[1].to_i)
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      def ev_rset_descriptor_table_size(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif s[1].to_i < 1024
          return send_data("CLIENT_ERROR length must be greater than 1024\r\n")
        end

        EM.set_descriptor_table_size(s[1].to_i)
        send_data("STORED\r\n")
      end

      # set_latency_avg_calc_rule <mode> <count> <command1> <command2>....
      # <mode> is on/off
      # <count> is denominator to calculate average.
      # <commandx> is target command
      def ev_set_latency_avg_calc_rule(s)
        #check argument
        if /^on$|^off$/ !~ s[1]
          return send_data("CLIENT_ERROR argument 1: please input \"on\" or \"off\"\r\n")
        elsif s[1] == "on" && (s.length <= 3 || s[2].to_i < 1)
          return send_data("CLIENT_ERROR number of arguments (0 for 3) and <count> must be greater than zero\r\n")
        elsif s[1] == "off" && !(s.length == 2)
          return send_data("CLIENT_ERROR number of arguments (0 for 1, or more 3)\r\n")
        end

        #check support commands
        s.each_index {|idx|
          if idx >= 3 && (!Event::Handler::ev_list.include?(s[idx]) || Event::Handler::system_commands.include?(s[idx]))
             return send_data("NOT SUPPORT [#{s[idx]}] command\r\n")
          end
        }

        arg ="rset_latency_avg_calc_rule"
        s.each_index {|idx|
          arg += " #{s[idx]}" if idx>=1
        }
        res = broadcast_cmd("#{arg}\r\n")

        if s[1] =="on"
          @stats.latency_check_cmd = [] #reset
          s.each_index {|idx|
            @stats.latency_check_cmd.push(s[idx]) if idx >= 3
          }
          @stats.latency_check_time_count = s[2].to_i
          @stats.latency_log = true
          res[@stats.ap_str] = "ACTIVATED"
        elsif s[1] =="off"
          @stats.latency_check_cmd = [] #reset
          @stats.latency_check_time_count = false
          @stats.latency_log = false
          res[@stats.ap_str] = "DEACTIVATED"
        end
        @stats.latency_data = Hash.new { |hash,key| hash[key] = {}}
        send_data("#{res}\r\n")
      end

      def ev_rset_latency_avg_calc_rule(s)
        if /^on$|^off$/ !~ s[1]
          return send_data("CLIENT_ERROR argument 1: please input \"on\" or \"off\"\r\n")
        elsif s[1] == "on" && (s.length <= 3 || s[2].to_i < 1)
          return send_data("CLIENT_ERROR number of arguments (0 for 3) and <count> must be greater than zero\r\n")
        elsif s[1] == "off" && !(s.length == 2)
          return send_data("CLIENT_ERROR number of arguments (0 for 1, or more 3)\r\n")
        end

        s.each_index {|idx|
          if idx >= 3 && (!Event::Handler::ev_list.include?(s[idx]) || Event::Handler::system_commands.include?(s[idx]))
             return send_data("NOT SUPPORT [#{s[idx]}] command\r\n")
          end
        }

        if s[1] =="on"
          @latency_data = Hash.new { |hash,key| hash[key] = {}}
          @stats.latency_check_cmd = []
          s.each_index {|idx|
            @stats.latency_check_cmd.push(s[idx]) if idx >= 3
          }
          @stats.latency_check_time_count = s[2].to_i
          @stats.latency_log = true
          send_data("ACTIVATED\r\n")
        elsif s[1] =="off"
          @latency_data = Hash.new { |hash,key| hash[key] = {}}
          @stats.latency_check_cmd = []
          @stats.latency_check_time_count = false
          @stats.latency_log = false
          send_data("DEACTIVATED\r\n")
        end
      end

      # add_calc_latency_average <command1> <command2>....
      def ev_add_latency_avg_calc_cmd(s)
        #check argument
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 2)\r\n")
        end
        #check support commands
        s.each_index {|idx|
          if idx >= 1 && (!Event::Handler::ev_list.include?(s[idx]) || Event::Handler::system_commands.include?(s[idx]))
             return send_data("NOT SUPPORT [#{s[idx]}] command\r\n")
          end
          if idx >= 1 && @stats.latency_check_cmd.include?(s[idx])
            return send_data("ALREADY SET [#{s[idx]}] command\r\n")
          end
        }

        arg ="radd_latency_avg_calc_cmd"
        s.each_index {|idx|
          arg += " #{s[idx]}" if idx>=1
        }
        res = broadcast_cmd("#{arg}\r\n")

        s.each_index {|idx|
          @stats.latency_check_cmd.push(s[idx]) if idx >= 1
        }
        res[@stats.ap_str] = "SET"
        send_data("#{res}\r\n")
      end

      def ev_radd_latency_avg_calc_cmd(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 2)\r\n")
        end
        s.each_index {|idx|
          if idx >= 2 && (!Event::Handler::ev_list.include?(s[idx]) || Event::Handler::system_commands.include?(s[idx]))
             return send_data("NOT SUPPORT [#{s[idx]}] command\r\n")
          end
          if idx >= 1 && @stats.latency_check_cmd.include?(s[idx])
            return send_data("ALREADY SET [#{s[idx]}] command\r\n")
          end
        }

        s.each_index {|idx|
          @stats.latency_check_cmd.push(s[idx]) if idx >= 1
        }
        send_data("SET\r\n")
      end

      # del_calc_latency_average <command1> <command2>....
      def ev_del_latency_avg_calc_cmd(s)
        #check argument
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 2)\r\n")
        end

        #check support commands
        s.each_index {|idx|
          if idx >= 1 && !@stats.latency_check_cmd.include?(s[idx])
            return send_data("[#{s[idx]}] command is NOT set\r\n")
          end
        }

        arg ="rdel_latency_avg_calc_cmd"
        s.each_index {|idx|
          arg += " #{s[idx]}" if idx>=1
        }
        res = broadcast_cmd("#{arg}\r\n")

        s.each_index {|idx|
          @stats.latency_check_cmd.delete(s[idx]) if idx >= 1
          @stats.latency_data.delete(s[idx]) if idx >= 1
        }
        res[@stats.ap_str] = "DELETED"
        send_data("#{res}\r\n")
      end

      def ev_rdel_latency_avg_calc_cmd(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 2)\r\n")
        end

        # reset
        s.each_index {|idx|
          if idx >= 1 && !@stats.latency_check_cmd.include?(s[idx])
            return send_data("[#{s[idx]}] command is NOT set\r\n")
          end
        }
        s.each_index {|idx|
          @stats.latency_check_cmd.delete(s[idx]) if idx >= 1
          @stats.latency_data.delete(s[idx]) if idx >= 1
        }
        send_data("DELETED\r\n")
      end

      # chg_calc_latency_average_denominator <count>
      def ev_chg_latency_avg_calc_time_count(s)
        #check argument
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments (0 for 2)\r\n")
        elsif s[1] != "nil" && s[1].to_i < 1
          return send_data("s[1].class = #{s[1].class}\r\n")
          return send_data("<count> must be greater than zero or nil[DEACTIVATE]\r\n")
        end

        res = broadcast_cmd("rchg_latency_avg_calc_time_count #{s[1]}\r\n")

        if s[1] != "nil"
          @stats.latency_check_time_count = s[1].to_i
          @stats.latency_log = true
        elsif s[1] == "nil"
          @stats.latency_check_time_count = false
          @stats.latency_log = false
        end
        res[@stats.ap_str] = "CHANGED"
        send_data("#{res}\r\n")
      end

      def ev_rchg_latency_avg_calc_time_count(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments (0 for 2)\r\n")
        elsif s[1] != "nil" && s[1].to_i < 1
          return send_data("<count> must be greater than zero\r\n")
        end

        if s[1] != "nil"
          @stats.latency_check_time_count = s[1].to_i
          @stats.latency_log = true
        elsif s[1] == "nil"
          @stats.latency_check_time_count = false
          @stats.latency_log = false
        end
        @stats.latency_check_time_count = s[1].to_i
        send_data("CHANGED\r\n")
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

      # set_connection_pool_expire_time <sec>
      # set to expired time(sec) for connection_pool expire time
      def ev_set_connection_pool_expire_time(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end

        res = broadcast_cmd("rset_connection_pool_expire_time #{s[1]}\r\n")
        Messaging::ConPool.instance.expire_time = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_connection_pool_expire_time <sec>
      def ev_rset_connection_pool_expire_time(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        Messaging::ConPool.instance.expire_time = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_emconnection_pool_expire_time <sec>
      def ev_set_emconnection_pool_expire_time(s)
        # chcking s incude command and value (NOT check digit)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end

        #if ARGV is 0, expire time become infinity(NOT happen expire)
        if s[1].to_i == 0
          s[1] = "2147483647"
        end
        res = broadcast_cmd("rset_emconnection_pool_expire_time #{s[1]}\r\n")
        Event::EMConPool::instance.expire_time = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_emconnection_pool_expire_time <sec>
      def ev_rset_emconnection_pool_expire_time(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        Event::EMConPool::instance.expire_time = s[1].to_i
        send_data("STORED\r\n")
      end

      # switch_dns_caching <on|off|true|false>
      def ev_switch_dns_caching(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end

        res = broadcast_cmd("rswitch_dns_caching #{s[1]}\r\n")
        if s[1] == 'on' || s[1] == 'true'
          DNSCache.instance.enable_dns_cache
          @log.info("DNS caching enabled")
          res[@stats.ap_str] = "ENABLED"
        elsif s[1] == 'off' || s[1] == 'false'
          DNSCache.instance.disable_dns_cache
          @log.info("DNS caching disabled")
          res[@stats.ap_str] = "DISABLED"
        else
          res[@stats.ap_str] = "NOTSWITCHED"
        end
        send_data("#{res}\r\n")
      end

      # rswitch_dns_caching <on|off|true|false>
      def ev_rswitch_dns_caching(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end

        if s[1] == 'on' || s[1] == 'true'
          DNSCache.instance.enable_dns_cache
          @log.info("DNS caching enabled")
          return send_data("ENABLED\r\n")
        elsif s[1] == 'off' || s[1] == 'false'
          DNSCache.instance.disable_dns_cache
          @log.info("DNS caching disabled")
          return send_data("DISABLED\r\n")
        else
          send_data("NOTSWITCHED\r\n")
        end
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

      # set_routing_trans_timeout <sec>
      def ev_set_routing_trans_timeout(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_f <= 0
          return send_data("CLIENT_ERROR time value must be lager than 0\r\n")
        end
        res = broadcast_cmd("rset_routing_trans_timeout #{s[1]}\r\n")
        @stats.routing_trans_timeout = s[1].to_f
        res[@stats.ap_str] = "STORED"

        send_data("#{res}\r\n")
      end

      # rset_set_routing_trans_timeout <sec>
      def ev_rset_routing_trans_timeout(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_f <= 0
          return send_data("CLIENT_ERROR time value must be lager than 0\r\n")
        end
        @stats.routing_trans_timeout = s[1].to_f

        send_data("STORED\r\n")
      end

      # set_spushv_read_timeout <sec>
      def ev_set_spushv_read_timeout(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_i <= 0
          return send_data("CLIENT_ERROR time value must be lager than 0\r\n")
        end
        res = broadcast_cmd("rset_spushv_read_timeout #{s[1]}\r\n")
        @stats.spushv_read_timeout = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_spushv_read_timeout <sec>
      def ev_rset_spushv_read_timeout(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_i <= 0
          return send_data("CLIENT_ERROR time value must be lager than 0\r\n")
        end
        @stats.spushv_read_timeout = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_reqpushv_timeout_count <sec>
      def ev_set_reqpushv_timeout_count(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_i <= 0
          return send_data("CLIENT_ERROR time value must be lager than 0\r\n")
        end
        res = broadcast_cmd("rset_reqpushv_timeout_count #{s[1]}\r\n")
        @stats.reqpushv_timeout_count = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # ev_rset_reqpushv_timeout_count <sec>
      def ev_rset_reqpushv_timeout_count(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_i <= 0
          return send_data("CLIENT_ERROR time value must be lager than 0\r\n")
        end
        @stats.reqpushv_timeout_count = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_spushv_klength_warn <byte>
      def ev_set_spushv_klength_warn(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_i <= 0
          return send_data("CLIENT_ERROR size value must be larger than 0 \r\n")
        end
        res = broadcast_cmd("rset_spushv_klength_warn #{s[1]}\r\n")
        @stats.spushv_klength_warn = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_set_spushv_klength_warn <byte>
      def ev_rset_spushv_klength_warn(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_i <= 0
          return send_data("CLIENT_ERROR size value must be larger than 0 \r\n")
        end
        @stats.spushv_klength_warn = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_spushv_vlength_warn <byte>
      def ev_set_spushv_vlength_warn(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_i <= 0
          return send_data("CLIENT_ERROR size value must be larger than 0 \r\n")
        end
        res = broadcast_cmd("rset_spushv_vlength_warn #{s[1]}\r\n")
        @stats.spushv_vlength_warn = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_set_spushv_vlength_warn <byte>
      def ev_rset_spushv_vlength_warn(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1].to_i <= 0
          return send_data("CLIENT_ERROR size value must be larger than 0\r\n")
        end
        @stats.spushv_vlength_warn = s[1].to_i
        send_data("STORED\r\n")
      end

      # wb_command_map <hash string>
      # ex.
      # {:set=>1,:append=>2,:delete=>3}
      def ev_wb_command_map(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        map = {}
        cmd = s[1..-1].join
        if cmd =~ /^\{(.+)\}$/
          $1.split(',').each do |kv|
            k, v = kv.split('=>')
            map[k[1..-1].to_sym] = v.to_i if v && k[0]==':'
          end

          res = broadcast_cmd("rwb_command_map #{s[1..-1].join}\r\n")
          @stats.wb_command_map = map
          res[@stats.ap_str] = map.inspect
          send_data("#{res}\r\n")
        else
          send_data("CLIENT_ERROR hash string parse error\r\n")
        end
      end

      def ev_rwb_command_map(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        end
        map = {}
        cmd = s[1..-1].join
        if cmd =~ /^\{(.+)\}$/
          $1.split(',').each do |kv|
            k, v = kv.split('=>')
            map[k[1..-1].to_sym] = v.to_i if v && k[0]==':'
          end
          @stats.wb_command_map = map
          send_data("#{map}\r\n")
        else
          send_data("CLIENT_ERROR hash string parse error\r\n")
        end
      end

      # set_wb_shift_size <size>
      def ev_set_wb_shift_size(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif s[1].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end

        res = broadcast_cmd("rset_wb_shift_size #{s[1]}\r\n")
        $roma.wb_writer.shift_size = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      def ev_rset_wb_shift_size(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif s[1].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end

        $roma.wb_writer.shift_size = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_storage_status [number of file][safecopy|normal]{hash_name}
      def ev_set_storage_status(s)
        if s.length < 3
          return send_data("CLIENT_ERROR number of arguments (#{s.length - 1} for 2)\r\n")
        end

        if s.length >= 4
          hname = s[3]
        else
          hname = 'roma'
        end
        st = @storages[hname]
        unless st
          return send_data("CLIENT_ERROR hash_name = #{hanme} does not found\r\n")
        end
        dn = s[1].to_i
        if st.divnum <= dn
          return send_data("CLIENT_ERROR divnum <= #{dn}\r\n")
        end
        if s[2] == 'safecopy'
          if st.dbs[dn] != :normal
            return send_data("CLIENT_ERROR storage[#{dn}] != :normal status\r\n")
          end
          if st.set_db_stat(dn, :safecopy_flushing) == false
            return send_data("CLIENT_ERROR storage[#{dn}] status can't changed\r\n")
          end
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_storage_flush_process',[hname, dn]))
        elsif s[2] ==  'normal'
          if st.dbs[dn] != :safecopy_flushed
            return send_data("CLIENT_ERROR storage[#{dn}] != :safecopy_flushed status\r\n")
          end
          if st.set_db_stat(dn, :cachecleaning) == false
            return send_data("CLIENT_ERROR storage[#{dn}] status can't changed\r\n")
          end
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_storage_cachecleaning_process',[hname, dn]))
        else
          return send_data("CLIENT_ERROR status parse error\r\n")
        end

        send_data("PUSHED\r\n")
      end

      # set_gui_run_snapshot [true|false]
      def ev_set_gui_run_snapshot(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end

        case s[1]
        when 'true'
          @stats.gui_run_snapshot = true
          send_data("STORED\r\n")
        when 'false'
          @stats.gui_run_snapshot = false
          send_data("STORED\r\n")
        else
          return send_data("CLIENT_ERROR value must be true or false\r\n")
        end
      end

      # set_gui_last_snapshot_date [%Y/%m/%d %H:%M:%S]
      def ev_set_gui_last_snapshot(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1] !~ /^\d+\/\d+\/\d+T\d+:\d+:\d+$/
          return send_data("CLIENT_ERROR format is [%Y/%m/%dT%H:%M:%S]\r\n")
        end
        res = broadcast_cmd("rset_gui_last_snapshot #{s[1]}\r\n")
        @stats.gui_last_snapshot = s[1]
        res[@stats.ap_str] = "PUSHED"
        send_data("#{res}\r\n")
      end

      # rset_gui_last_snapshot(s)
      def ev_rset_gui_last_snapshot(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\n\r")
        end
        if s[1] !~ /^\d+\/\d+\/\d+T\d+:\d+:\d+$/
          return send_data("CLIENT_ERROR format is [%Y/%m/%dT%H:%M:%S]\r\n")
        end
        @stats.gui_last_snapshot = s[1]
        send_data("PUSHED\r\n")
      end

      # set_cleanup_regexp <regexp>
      def ev_set_cleanup_regexp(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments #{s.length-1} to 1\r\n")
        end

        # failover check
        unless @rttable.enabled_failover
          return send_data("CLIENT_ERROR failover disable now!!\r\n")
        end

        @storages.each{|hname,st|
          st.cleanup_regexp = s[1]
          st.stop_clean_up
          send_data("STORED\r\n")
        }
      end

      # set_log_shift_size <size>
      def ev_set_log_shift_size(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif s[1].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end

        res = broadcast_cmd("rset_log_shift_size #{s[1]}\r\n")
        @log.set_log_shift_size(s[1].to_i)
        @stats.log_shift_size = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      def ev_rset_log_shift_size(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif s[1].to_i < 1
          return send_data("CLIENT_ERROR length must be greater than zero\r\n")
        end

        @log.set_log_shift_size(s[1].to_i)
        @stats.log_shift_size = s[1].to_i
        send_data("STORED\r\n")
      end

      # set_log_shift_age <age>
      def ev_set_log_shift_age(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif s[1].to_i < 1 && !['0', 'min', 'hour', 'daily', 'weekly', 'monthly'].include?(s[1])
          return send_data("CLIENT_ERROR invalid arguments\r\n")
        end

        res = broadcast_cmd("rset_log_shift_age #{s[1]}\r\n")

        if s[1].to_i > 0 || s[1] == '0'
          @log.set_log_shift_age(s[1].to_i)
          @stats.log_shift_age = s[1].to_i
        else
          @log.set_log_shift_age(s[1])
          @stats.log_shift_age = s[1]
        end
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      def ev_rset_log_shift_age(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments\r\n")
        elsif s[1].to_i < 1 && !['0', 'min', 'hour', 'daily', 'weekly', 'monthly'].include?(s[1])
          return send_data("CLIENT_ERROR invalid arguments\r\n")
        end

        if s[1].to_i > 0 || s[1] == '0'
          @log.set_log_shift_age(s[1].to_i)
          @stats.log_shift_age = s[1].to_i
        else
          @log.set_log_shift_age(s[1])
          @stats.log_shift_age = s[1]
        end
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
        @stats.dcnice = p
        "STORED"
      end

      def get_config_stat
        ret = {}
        ret['config.DEFAULT_LOST_ACTION'] = Config::DEFAULT_LOST_ACTION
        ret['config.LOG_SHIFT_AGE'] = Config::LOG_SHIFT_AGE
        ret['config.LOG_SHIFT_SIZE'] = Config::LOG_SHIFT_SIZE
        ret['config.LOG_PATH'] = File.expand_path(Config::LOG_PATH)
        ret['config.RTTABLE_PATH'] = File.expand_path(Config::RTTABLE_PATH)
        ret['config.STORAGE_DELMARK_EXPTIME'] = Config::STORAGE_DELMARK_EXPTIME
        if Config.const_defined?(:STORAGE_EXCEPTION_ACTION)
          ret['config.STORAGE_EXCEPTION_ACTION'] = Config::STORAGE_EXCEPTION_ACTION
        end
        ret['config.DATACOPY_STREAM_COPY_WAIT_PARAM'] = Config::DATACOPY_STREAM_COPY_WAIT_PARAM
        ret['config.PLUGIN_FILES'] = Config::PLUGIN_FILES.inspect
        ret['config.WRITEBEHIND_PATH'] = File.expand_path(Config::WRITEBEHIND_PATH)
        ret['config.WRITEBEHIND_SHIFT_SIZE'] = Config::WRITEBEHIND_SHIFT_SIZE
        if Config.const_defined?(:CONNECTION_DESCRIPTOR_TABLE_SIZE)
          ret['config.CONNECTION_DESCRIPTOR_TABLE_SIZE'] = Config::CONNECTION_DESCRIPTOR_TABLE_SIZE
        end
        ret
      end

    end # module SystemCommandReceiver
  end # module Command
end # module Roma

