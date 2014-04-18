
module Roma
  module CommandPlugin

    module PluginGui
      include ::Roma::CommandPlugin

      def ev_get_routing_history(s)
        routing_path  = get_config_stat["config.RTTABLE_PATH"]
        f_list = Dir.glob("#{routing_path}/*")
        contents = ""
        f_list.each{|fname|
          contents << File.read(fname)
        }
        routing_list = contents.scan(/[-\.a-zA-Z\d]+_[\d]+/).uniq
        routing_list.each{|routing|
          send_data("#{routing}\r\n")
        }
        send_data("END\r\n")
      end

    end
  end
end
