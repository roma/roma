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

      ### get_logs [line count]
      ### get_logs [start_date] [end_date]
      # gather_logs [start_date(YYYY-MM-DDThh:mm:ss)]
      def ev_gather_logs(s)
        #if s.length != 2
        #if s.length < 1 || s.length > 3
        if s.length != 2
          return send_data("CLIENT_ERROR number of arguments (#{s.length-1} for 1)\r\n")
        end

        start_date = s[1]

        ###[todo] end date
        #if s.length = 2
        #  end_date = 'end'
        #else
        #  end_date = s[2]
        #end

        ###[todo] log level
        #if s.length == 4
        #   log_level = s[3]
        #else
        #   log_level = nil
        #end

        ### line count  
        #line_count = s[1].to_i
        #if line_count < 1 || line_count > 100
        #  return send_data("CLIENT_ERROR line counts is restricted to between 1-100 lines\r\n")
        #end

        if @stats.gui_run_gather_logs
          return send_data("CLIENT_ERROR gathering process is already going\r\n")
        end

        begin
          @stats.gui_run_gather_logs = true
          #Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_get_logs', [line_count]))
          #Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_get_logs', [start_date, end_date, log_level]))
          #Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_get_logs', [start_date, end_date]))
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_get_logs', [start_date]))

          send_data("STARTED\r\n")
        rescue
          @stats.gui_run_gather_logs = false
          @rttable.logs = []
          send_data("CLIENT_ERROR\r\n")
        end
      end

      # show_logs
      def ev_show_logs(s)
        if @stats.gui_run_gather_logs
          send_data("Not finished gathering\r\n")
        else
          @rttable.logs.each{|log|
            send_data(log)
          }
          send_data("END\r\n")
          @rttable.logs.clear
        end
      end

    end # end of module PluginGui
  end # end of module CommandPlugin
end # end of modlue Roma
