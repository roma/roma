require 'roma/messaging/con_pool'
require 'roma/async_process'

module Roma
  module Command

    module VnodeCommandReceiver

      # pushv <hash-name> <vnode-id>
      # src                             dst
      #  |   ['pushv' <hname> <vn>\r\n]->|
      #  |<-['READY'\r\n]                |
      #  |               [<length>\r\n]->|
      #  |                 [<dump>\r\n]->|
      #  |                  ['END'\r\n]->|
      #  |<-['STORED'\r\n]               |
      def ev_pushv(s)
        send_data("READY\r\n")
        len = gets
        res = em_receive_dump(s[1], len.to_i)
        if res == true
          send_data("STORED\r\n")
        else
          send_data("SERVER_ERROR #{res}\r\n")
        end
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      # spushv <hash-name> <vnode-id>
      # src                             dst
      #  |  ['spushv' <hname> <vn>\r\n]->|
      #  |<-['READY'\r\n]                |
      #  |                 [<dumpdata>]->|
      #  |                       :       |
      #  |                       :       |
      #  |              [<end of dump>]->|
      #  |<-['STORED'\r\n]               |
      def ev_spushv(s)
        if s.length != 3
          @log.error("#{__method__}:wrong number of arguments(#{s})")
          return send_data("CLIENT_ERROR Wrong number of arguments.\r\n")
        end
        @stats.run_receive_a_vnode["#{s[1]}_#{s[2]}"] = true

        $roma.stop_clean_up

        send_data("READY\r\n")

        count = rcount = 0
        @log.debug("#{__method__}:#{s.inspect} received.")
        loop {
          context_bin = read_bytes(20, 100)
          vn, last, clk, expt, klen = context_bin.unpack('NNNNN')
          break if klen == 0 # end of dump ?
          k = read_bytes(klen)
          vlen_bin = read_bytes(4, 100)
          vlen, =  vlen_bin.unpack('N')
          if vlen != 0
            v = read_bytes(vlen, 100)

            createhash(s[1]) unless @storages[s[1]]
            if @storages[s[1]].load_stream_dump(vn, last, clk, expt, k, v)
              count += 1
#              @log.debug("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was stored.")
            else
              rcount += 1
#              @log.warn("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was rejected.")
            end
          else
            createhash(s[1]) unless @storages[s[1]]
            if @storages[s[1]].load_stream_dump(vn, last, clk, expt, k, nil)
#              @log.debug("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was stored.")
              count += 1
            else
              rcount += 1
#              @log.warn("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was rejected.")
            end
          end
        }
        send_data("STORED\r\n")
        @log.debug("#{__method__}:#{s[1]}_#{s[2]} #{count} keys loaded.")
        @log.debug("#{__method__}:#{s[1]}_#{s[2]} #{rcount} keys rejected.") if rcount > 0
      rescue Storage::StorageException => e
        @log.error("#{e.inspect} #{$@}")
        close_connection
        if Config.const_defined?(:STORAGE_EXCEPTION_ACTION) &&
            Config::STORAGE_EXCEPTION_ACTION == :shutdown
          @log.error("#{__method__}:Romad will be stop.")
          @stop_event_loop = true
        end
      rescue => e
        @log.error("#{e} #{$@}")
      ensure
        @stats.run_receive_a_vnode.delete("#{s[1]}_#{s[2]}") if s.length == 3
        @stats.last_clean_up = Time.now
      end      

      # reqpushv <vnode-id> <node-id> <is primary?>
      # src                                       dst
      #  |<-['reqpushv <vn> <nid> <p?>\r\n']         |
      #  |                           ['PUSHED'\r\n]->|
      def ev_reqpushv(s)
        if s.length!=4
          send_data("CLIENT_ERROR usage:reqpushv vnode-id node-id primary-flag(true/false)\r\n")
          return
        end
        if(@stats.run_iterate_storage || @stats.run_join)
          @log.warn("reqpushv rejected:#{s}")
          send_data("REJECTED\r\n")
          return
        end
        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('reqpushv',[s[1],s[2],s[3]]))
        send_data("PUSHED\r\n")
      rescue =>e
        @log.error("#{e}\n#{$@}")
      end

      def req_push_a_vnode(vn, src_nid, is_primary)
        con = Roma::Messaging::ConPool.instance.get_connection(src_nid)
        con.write("reqpushv #{vn} #{@nid} #{is_primary}\r\n")
        res = con.gets # receive 'PUSHED\r\n' | 'REJECTED\r\n'
        Roma::Messaging::ConPool.instance.return_connection(src_nid,con)
        # waiting for pushv
        count = 0
        while @rttable.search_nodes(vn).include?(@nid)==false && count < 300
          sleep 0.1
          count += 1
        end
      rescue =>e
        @log.error("#{e}\n#{$@}")
        @rttable.proc_failed(src_nid)
        false
      end

      def em_receive_dump(hname, len)
        dmp = read_bytes(len)
        read_bytes(2)
        if gets == "END\r\n"
          if @storages.key?(hname)
            n = @storages[hname].load(dmp)
            @log.debug("#{dmp.length} bytes received.(#{n} keys loaded.)")
            return true
          else
            @log.error("receive_dump:@storages[#{hname}] dose not found.")
            return "@storages[#{hname}] dose not found."
          end
        else
          @log.error("receive_dump:END was not able to be received.")
          return "END was not able to be received."
        end
      rescue =>e
        @log.error("#{e}\n#{$@}")
        "#{e}"
      end
      private :em_receive_dump

    end # module VnodeCommandReceiver

  end # module Command
end # module Roma
