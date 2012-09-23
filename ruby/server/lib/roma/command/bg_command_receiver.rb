# -*- coding: utf-8 -*-
require 'roma/async_process'
require 'roma/messaging/con_pool'
require 'roma/command/vn_command_receiver'

module Roma
  module Command

    module BackgroundCommandReceiver
      include VnodeCommandReceiver

      def ev_balance(s)
        res = broadcast_cmd("rbalance\r\n")
        if @stats.run_recover==false && 
            @stats.run_acquire_vnodes == false &&
            @rttable.vnode_balance(@stats.ap_str)==:less
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_acquire_vnodes_process'))
          res[@stats.ap_str] = 'STARTED'
        else
          res[@stats.ap_str] = 'SERVER_ERROR Not unbalance or Balance/Recover/Sync process is already running.'
        end
        send_data("#{res}\r\n")
      end

      def ev_rbalance(s)
        if @stats.run_recover==false && 
            @stats.run_acquire_vnodes == false &&
            @rttable.vnode_balance(@stats.ap_str)==:less
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_acquire_vnodes_process'))
          send_data("STARTED\r\n")
        else
          send_data("SERVER_ERROR Not unbalance or Balance/Recover/Sync process is already running.\r\n")
        end
      end

      def ev_release(s)
        if @stats.run_recover==false && 
            @stats.run_acquire_vnodes == false &&
            @stats.run_release == false &&
            @stats.run_iterate_storage == false
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_release_process'))
          send_data("STARTED\r\n")
        else
          send_data("SERVER_ERROR Release/Balance/Recover/Sync process is already running.\r\n")
        end
      end

      # recover
      def ev_recover(s)
        if @rttable.can_i_recover?
          cmd = "rrecover"
          res = broadcast_cmd("#{cmd}\r\n")
          unless @stats.run_recover
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_recover_process'))
            res[@nid] = "STARTED"
          else
            res[@nid] = "SERVER_ERROR Recover/Sync process is already running."
          end
          send_data("#{res}\r\n")
        else
          send_data("SERVER_ERROR nodes num < redundant num\r\n")
        end
      end

      # rrecover
      def ev_rrecover(s)
        if @rttable.can_i_recover?
          unless @stats.run_recover
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_recover_process'))
            send_data("STARTED\r\n")
          else
            send_data("SERVER_ERROR Recover process is already running.\r\n")
          end
        else
          send_data("SERVER_ERROR nodes num < redundant num\r\n")
        end
      end

      # sync <hname>
      def ev_sync(s)
        res = nil
        if s.length==1
          res = broadcast_cmd("rsync\r\n")
        else
          res = broadcast_cmd("rsync #{s[1]}\r\n")
        end
        unless @stats.run_recover
          if s.length==1
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_sync_process',@storages.keys))
          else
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_sync_process',[s[1]]))
          end
          res[@nid] = "STARTED"
        else
          res[@nid] = "SERVER_ERROR Recover/Sync process is already running."
        end
        send_data("#{res}\r\n")
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      # rsync <hname>
      def ev_rsync(s)
        unless @stats.run_recover
          if s.length==1
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_sync_process',@storages.keys))
          else
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_sync_process',[s[1]]))
          end
          send_data("STARTED\r\n")
        else
          send_data("SERVER_ERROR Recover/Sync process is already running.\r\n")
        end
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      # dumpfile <key> <path>
      def ev_dumpfile(s)
        if s.length != 3
          send_data("CLIENT_ERROR usage:dumpfile <key> <path>\r\n")
          return
        end

        res = broadcast_cmd("rdumpfile #{s[1]} #{s[2]}\r\n")
        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_dumpfile_process',[s[1],s[2],:dumpfile]))
        path = Roma::Config::STORAGE_DUMP_PATH + '/' + s[2]
        res[@nid] = "STARTED #{path}/#{@nid}"
        send_data("#{res}\r\n")
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      # rdumpfile <key> <path>
      def ev_rdumpfile(s)
        if s.length != 3
          send_data("CLIENT_ERROR usage:rdumpfile <key> <path>\r\n")
          return
        end
        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_dumpfile_process',[s[1],s[2],:rdumpfile]))
        path = Roma::Config::STORAGE_DUMP_PATH + '/' + s[2]
        send_data("STARTED #{path}/#{@nid}\r\n")
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      def acquire_vnodes
        count = 0
        nv = @rttable.v_idx.length
        while (@rttable.vnode_balance(@nid) == :less && count < nv) do
          count += 1
          break unless acquire_vnode
        end
        @log.info("acquire_vnodes has done.")
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      def acquire_vnode
        widthout_nodes = @rttable.nodes

        unless @stats.enabled_repetition_host_in_routing
          myhost = @stats.ap_str.split(/[:_]/)[0]
          widthout_nodes.delete_if{|nid| nid.split(/[:_]/)[0] != myhost }
        else
          widthout_nodes = [@stats.ap_str]
        end

        vn, nodes = @rttable.sample_vnode(widthout_nodes)
        unless vn
          @log.warn("acquire_vnode:sample_vnode dose not found")
          return false
        end
        #
        # tunning point
        # sleep 0.1
        #
        req_push_a_vnode(vn, nodes[0], rand(@rttable.rn) == 0)
        true
      end

    end # module BackgroundCommandReceiver

  end # module Command
end # module Roma
