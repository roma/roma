
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

      def get_key_info(key)
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        s = sprintf("d = %s 0x%x\r\n",d,d)
        send_data(s)
        s = sprintf("vn = %s 0x%x\r\n",vn,vn)
        send_data(s)
        send_data("nodes = #{nodes.inspect}\r\n")
        "END"
      end

    end
  end
end
