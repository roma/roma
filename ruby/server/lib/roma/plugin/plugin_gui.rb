require 'json'

module Roma
  module CommandPlugin

    module PluginGui
      include ::Roma::CommandPlugin

      #[ToDO] change to background process
      def ev_get_routing_history(s)
        routing_path  = get_config_stat["config.RTTABLE_PATH"]
        f_list = Dir.glob("#{routing_path}/*")
        contents = ""
        f_list.each{|fname|
          contents << File.read(fname)
        }
        routing_list = contents.scan(/[-\.a-zA-Z\d]+_[\d]+/).uniq.sort
        routing_list.each{|routing|
          send_data("#{routing}\r\n")
        }
        send_data("END\r\n")
      end

      def ev_enabled_repetition_host_in_routingdump(s)
        rt = @rttable
        rd = @rttable.sub_nid_rd(@addr)
        rt = Roma::Routing::RoutingTable.new(rd) if rd

        dmp = rt.dump_json
        hash = JSON.parse(dmp)

        repetition = false
        hash[2].each_value{|value|
          host = []
          value.map{|instance|
            host << instance.split("_")[0]
          }
          if host.uniq!
            repetition = true
            break
          end
        }
        if repetition
          send_data("true\r\n")
        else
          send_data("false\r\n")
        end
      end

      #[ToDO] change to background process
      # get_logs [line count]
      def ev_get_logs(s)
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments (#{s.length-1} for 1)\r\n")
        end

        get_line_count = s[1].to_i
        log_path = get_config_stat["config.LOG_PATH"]
        log_file = "#{log_path}/#{@stats.ap_str}.log"

        raw_logs = []
        f = File.new(log_file)
        f.each_line{|line|
          raw_logs << line
        }

        sliced_logs = []
        if raw_logs.size > get_line_count
          sliced_logs = raw_logs.slice(-get_line_count..-1)
        else
          sliced_logs = raw_logs
        end

        sliced_logs.each{|line|
          send_data(line)
        }

        send_data("END\r\n")
      end

    end
  end
end
