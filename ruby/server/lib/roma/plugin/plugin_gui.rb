require 'json'

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
        routing_list = contents.scan(/[-\.a-zA-Z\d]+_[\d]+/).uniq.sort
        routing_list.each{|routing|
          send_data("#{routing}\r\n")
        }
        send_data("END\r\n")
      end

      def ev_enabled_repetition_host_in_routingdump(s)
        rcv = send_cmd(@stats.ap_str, "routingdump json\r\n")
        hash = JSON.parse(rcv)
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
    end
  end
end
