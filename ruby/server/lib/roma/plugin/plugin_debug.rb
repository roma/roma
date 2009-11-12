
module Roma
  module CommandPlugin

    module PluginOperation
      include ::Roma::CommandPlugin

      # DANGER!!
      def ev_eval(s)
        cmd = s[1..-1].join(' ')
        @log.debug("eval(#{cmd})")
        send_data("#{eval(cmd)}\r\n")
      rescue =>e
        send_data("#{e}\r\n")
      end

    end
  end
end
