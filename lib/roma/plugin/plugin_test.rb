
module Roma
  module CommandPlugin

    module PluginTest
      include ::Roma::CommandPlugin

      def ev_echo(s)
        send_data("#{s.inspect}\r\n")
      end

    end
  end
end
