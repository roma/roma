require 'json'

module Roma
  module CommandPlugin

    module PluginGui
      include ::Roma::CommandPlugin

      # get_routing_history
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

      #[ToDO] have to change logic
      # get_logs [line count] [LOG_LEVEL]
      def ev_get_logs(s)
        if s.length != 2 && s.length != 3
          return send_data("CLIENT_ERROR number of arguments (#{s.length-1} for 1..2)\r\n")
        end

        line_count = s[1].to_i
        log_level  = s[2].chomp if s[2]

        @stats.gui_run_gather_logs = true

        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_get_logs', [line_count, log_level]))

        begin
          50.times{|count|
            sleep 0.1
            break unless @stats.gui_run_gather_logs
            raise if count == 49
          }

          #send_data("#{@rttable.logs}\r\n")
          @rttable.logs.each{|log|
            send_data(log)
          }

          send_data("END\r\n")
        rescue
          send_data("CLIENT_ERROR\r\n")
        end
      end

    end
  end
end
