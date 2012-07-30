
module Roma
  module CommandPlugin

    module PluginTest
      include ::Roma::CommandPlugin

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

    end
  end
end
