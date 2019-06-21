require 'roma/messaging/con_pool'

module Roma
  module Command

    module UtilCommandReceiver

      def send_cmd(nid, cmd)
        con = get_connection(nid)
        con.send(cmd)
        res = con.gets
        if res == nil
          @rttable.proc_failed(nid)
          return nil
        elsif res.start_with?("ERROR") == false
          @rttable.proc_succeed(nid)
          return_connection(nid, con)
        end
        res.chomp
      rescue => e
        @rttable.proc_failed(nid)
        @logger.error("#{e}\n#{$@}")
        nil
      end

      def broadcast_cmd(cmd)
        res={}
        @rttable.nodes.each{|nid|
          res[nid] = send_cmd(nid,cmd) if nid != @stats.ap_str
        }
        res
      end

      def async_send_cmd(nid, cmd)
        con = Roma::Messaging::ConPool.instance.get_connection(nid)
        con.write(cmd)
        res = con.gets
        Roma::Messaging::ConPool.instance.return_connection(nid, con)
        if res
          res.chomp!
          @rttable.proc_succeed(nid)
        else
          @rttable.proc_failed(nid)
        end
        res
      rescue => e
        @rttable.proc_failed(nid)
        @logger.error("#{e}\n#{$@}")
        nil
      end

      def async_broadcast_cmd(cmd,without_nids=nil)
        without_nids=[@stats.ap_str] unless without_nids
        res = {}
        @rttable.nodes.each{ |nid|
          res[nid] = async_send_cmd(nid,cmd) unless without_nids.include?(nid)
        }
        res
      rescue => e
        @logger.error("#{e}\n#{$@}")
        nil
      end

      # change to actual time for a memcached's expire time value
      def chg_time_expt(expt)
        if expt == 0
          expt = 0x7fffffff
        elsif expt < 2592000
          expt += Time.now.to_i
        end
        expt
      end

    end # module UtilCommandReceiver

  end # module Command
end # module Roma
