# -*- coding: utf-8 -*-
require 'thread'
require 'digest/sha1'

module Roma
  
  class AsyncMessage
    attr_accessor :event
    attr_accessor :args
    attr_accessor :callback

    def initialize(ev,ag=nil,&cb)
      @event = ev
      @args = ag
      @callback = cb
      @retry_count = 0
      @retry_max = 10
      @retry_wait = 0.1
    end

    def retry?
      @retry_max > @retry_count
    end

    def incr_count
      @retry_count += 1
    end

    def wait
      sleep(@retry_wait)
    end
  end

  module AsyncProcess

    @@async_queue = Queue.new

    def self.queue
      @@async_queue
    end

    def start_async_process
      @async_thread = Thread.new{
        async_process_loop
      }
    rescue =>e
      @log.error("#{e}\n#{$@}")
    end

    private

    def stop_async_process
      count = 0
      while @@async_queue.empty? == false && count < 100
        count += 1
        sleep 0.1
      end
      @async_thread.exit
    end

    def async_process_loop
      loop {
        while msg = @@async_queue.pop
          if send("asyncev_#{msg.event}",msg.args)
            msg.callback.call(msg,true) if msg.callback
          else
            if msg.retry?
              Thread.new{
                msg.wait
                msg.incr_count
                @@async_queue.push(msg)
              }
            else
              @log.error("async process retry out:#{msg.inspect}")
              msg.callback.call(msg,false) if msg.callback
            end
          end
        end
      }
    rescue =>e
      @log.error("#{e}\n#{$@}")
      retry
    end

    def asyncev_broadcast_cmd(args)
      @log.debug("asyncev_broadcast_cmd #{args.inspect}")
      cmd, nids, tout = args
      Thread::new{
        async_broadcast_cmd("#{cmd}\r\n", nids, tout)
      }
      true
    end

    def asyncev_start_acquire_vnodes_process(args)
      @log.debug("asyncev_start_acquire_vnodes_process")
      if @stats.run_acquire_vnodes
        @log.error("asyncev_start_acquire_vnodes_process:already in being")
        return true
      end
      @stats.run_acquire_vnodes = true
      Thread::new{
        begin
          acquire_vnodes_process
        rescue =>e
          @log.error("asyncev_start_acquire_vnodes_process:#{e.inspect} #{$@}")
        ensure
          @stats.run_acquire_vnodes = false
          @stats.join_ap = nil
        end
      }
      true
    end

    def asyncev_start_dumpfile_process(args)
      @log.debug("asyncev_start_dumpfile_process #{args.inspect}")
      key, path, cmd = args
      path = Roma::Config::STORAGE_DUMP_PATH + '/' + path
      Thread.new{
        begin
          except_vnh = {}
          @rttable.each_vnode{|vn,nids|
            if nids[0]!=@stats.ap_str
              except_vnh[vn]=vn
            end
          }
          # cmd expect the :dumpfile or :rdumpfile. 
          delete_to_end_of_dump(key) if cmd == :dumpfile
          sleep 0.1
          @storages.each_pair{|hname,st|
            st.dump_file("#{path}/#{@stats.ap_str}/#{hname}",except_vnh)
          }
          ret = set_to_end_of_dump(key,@stats.ap_str)
          if ret==nil || ret!="STORED"
            @log.error("asyncev_start_dumpfile_process:result of set_to_end_of_dump was #{ret.inspect}")
          end
          @log.info("asyncev_start_dumpfile_process has done.")
        rescue =>e
          @log.error("#{e}\n#{$@}")
        end
      }
      true
    end

    def delete_to_end_of_dump(key)
      con = Roma::Messaging::ConPool.instance.get_connection(@stats.ap_str)
      con.write("delete #{key}\eroma\r\n")
      res = con.gets
      res.chomp! if res
      Roma::Messaging::ConPool.instance.return_connection(@stats.ap_str,con)
      res
    end

    def set_to_end_of_dump(key,nid)
      count = 0
      begin
        sleep 0.1
        con = Roma::Messaging::ConPool.instance.get_connection(nid)
        con.write("add #{key}\eroma 0 86400 #{nid.length}\r\n#{nid}\r\n")
        res = con.gets
        unless res=="STORED\r\n"
          con.write("append #{key}\eroma 0 86400 #{nid.length+1}\r\n,#{nid}\r\n")
          res = con.gets
        end
        Roma::Messaging::ConPool.instance.return_connection(nid,con)
        count += 1
      end while(res!="STORED\r\n" && count < 5)
      res.chomp! if res
      res
    end

    def asyncev_redundant(args)
      nid,hname,k,d,clk,expt,v = args
      @log.debug("asyncev_redundant #{args.inspect}")
      unless @rttable.nodes.include?(nid)
        @log.warn("async redundant failed:#{nid} dose not found in routing table.#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}")
        return true # no retry
      end
      res = async_send_cmd(nid,"rset #{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}\r\n#{v}\r\n",10)
      unless res
        @log.warn("async redundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_zredundant(args)
      nid,hname,k,d,clk,expt,zv = args
      @log.debug("asyncev_zredundant #{args.inspect}")
      unless @rttable.nodes.include?(nid)
        @log.warn("async zredundant failed:#{nid} dose not found in routing table.#{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}")
        return true # no retry
      end
      res = async_send_cmd(nid,"rzset #{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}\r\n#{zv}\r\n",10)
      unless res
        @log.warn("async zredundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_reqpushv(args)
      vn, nid, p = args
      @log.debug("asyncev_reqpushv #{args.inspect}")
      Thread::new{
        sync_a_vnode(vn.to_i, nid, p == 'true')
      }
    end

    def asyncev_start_recover_process(args)
      @log.debug("asyncev_start_recover_process #{args.inspect}")
      @stats.run_recover = true
      Thread::new{
        begin
          if args != nil
            acquired_recover_process
          elsif args[0] == '-s'
            recover_process
          elsif args[0] == '-r' || args[0] == nil
            acquired_recover_process
          else
            @log.error("asyncev_start_recover_process:argument error #{args.inspect}")
          end
        rescue => e
          @log.error("asyncev_start_recover_process:#{e.inspect} #{$@}")
        end
        @stats.run_recover = false
      }
    end

    def asyncev_start_release_process(args)
      @log.debug("asyncev_start_release_process #{args}")
      @stats.run_release = true
      Thread::new{
        begin
          release_process
        rescue => e
          @log.error("asyncev_start_release_process:#{e.inspect} #{$@}")
        end
        @stats.run_relase = false
      }
    end

    def asyncev_start_sync_process(args)
      @log.debug("asyncev_start_sync_process")
      @stats.run_recover = true
      Thread::new{
        sync_process(args)
        @stats.run_recover = false
      }
    end

    def sync_process(st)
      own_nid = @stats.ap_str
      @do_sync_process = true
      @rttable.each_vnode{ |vn, nids|
        break unless @do_sync_process
        # my process charges of the primary node
        if nids[0] == own_nid
          nids[1..-1].each{ |nid|
            unless sync_a_vnode(vn, nid)
              @log.warn("sync_process:error at vn=#{vn} nid=#{nid}")
            end
          }
        end
      }
      @log.info("Sync process has done.")
    rescue =>e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @do_sync_process = false
      Roma::Messaging::ConPool.instance.close_all
    end

    def acquired_recover_process
      @log.info("acquired_recover_process:start")
      exclude_nodes = @rttable.nodes
      
      if @stats.enabled_repetition_host_in_routing
        exclude_nodes = [@stats.ap_str]
      else
        myhost = @stats.ap_str.split(/[:_]/)[0]
        exclude_nodes.delete_if{|nid| nid.split(/[:_]/)[0] != myhost }
      end
      
      @do_acquired_recover_process = true
      loop {
        break unless @do_acquired_recover_process
        vnodes = @rttable.select_a_short_vnodes(exclude_nodes)
        @log.info("acquired_recover_process:#{vnodes.length} short vnodes found.")
        break if vnodes.length == 0
        vn, nodes = vnodes[rand(vnodes.length)]
        if nodes.length != 0
          ret = req_push_a_vnode(vn, nodes[0], rand(@rttable.rn) == 0)
          if ret == :rejected
            sleep 1
          elsif ret == false
            break
          end
          sleep 1
        end
      }
      @log.info("acquired_recover_process has done.")
    rescue => e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @do_acquired_recover_process = false
    end
 
    def acquire_vnodes_process
      count = 0
      nv = @rttable.v_idx.length
      @do_acquire_vnodes_process = true
      while (@rttable.vnode_balance(@stats.ap_str) == :less && count < nv) do
        break unless @do_acquire_vnodes_process
        ret = acquire_vnode
        if ret == :rejected
          sleep 5
          next
        elsif ret == false
          break
        end
        sleep 1
        count += 1
      end
      @log.info("acquire_vnodes_prosess has done.")
    rescue => e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @do_acquire_vnodes_process = false
    end
 
    def acquire_vnode
      widthout_nodes = @rttable.nodes
      
      if @stats.enabled_repetition_host_in_routing
        widthout_nodes = [@stats.ap_str]
      else
        myhost = @stats.ap_str.split(/[:_]/)[0]
        widthout_nodes.delete_if{|nid| nid.split(/[:_]/)[0] != myhost }
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
    end
 
    def req_push_a_vnode(vn, src_nid, is_primary)
      con = Roma::Messaging::ConPool.instance.get_connection(src_nid)
      con.write("reqpushv #{vn} #{@stats.ap_str} #{is_primary}\r\n")
      res = con.gets # receive 'PUSHED\r\n' | 'REJECTED\r\n'
      if res == "REJECTED\r\n"
        @log.warn("req_push_a_vnode:request was rejected from #{src_nid}.")
        return :rejected
      end
      Roma::Messaging::ConPool.instance.return_connection(src_nid,con)
      # waiting for pushv
      count = 0
      while @rttable.search_nodes(vn).include?(@stats.ap_str)==false && count < 300
        sleep 0.1
        count += 1
      end
      if count >= 300
        @log.warn("req_push_a_vnode:request has been time-out.vn=#{vn} nid=#{src_nid}")
        return :timeout
      end
      true
    rescue =>e
      @log.error("req_push_a_vnode:#{e.inspect} #{$@}")
      @rttable.proc_failed(src_nid)
      false
    end
 
    def recover_process
      @log.info("recover_process:start.")
      nodes = @rttable.nodes
      
      unless @stats.enabled_repetition_host_in_routing
        host = @stats.ap_str.split(/[:_]/)[0]
        nodes.delete_if{|nid| nid.split(/[:_]/)[0] == host }
      else
        nodes.delete(@stats.ap_str)
      end
      
      if nodes.length == 0
        @log.error("New redundant node dose not found.")
        return
      end

      @do_recover_process = true
      @rttable.each_vnode{ |vn, nids|
        break unless @do_recover_process
        # my process charges of a primary node and it's short of redundant
        if nids[0] == @stats.ap_str && nids.length < @rttable.rn
          unless sync_a_vnode(vn, nodes[rand(nodes.length)])
            @log.warn("recover_process:error at hname=#{hname} vn=#{vn}")
          end
        end
      }
      @log.info("Recover process has done.")
    rescue =>e
      @log.error("#{e}\n#{$@}")
    ensure
      @do_recover_process = false
      Roma::Messaging::ConPool.instance.close_all
    end

    def release_process
      @log.info("release_process:start.")
      nodes = @rttable.nodes
      
      unless @stats.enabled_repetition_host_in_routing
        host = @stats.ap_str.split(/[:_]/)[0]
        nodes.delete_if{|nid| nid.split(/[:_]/)[0] == host }
      else
        nodes.delete(@stats.ap_str)
      end
      
      if nodes.length < @rttable.rn
        @log.error("Physcal node dose not found.")
        return
      end

      @do_release_process = true
      @rttable.each_vnode{ |vn, nids|
        break unless @do_release_process
        if nids.include?(@stats.ap_str)
          buf = nodes.clone

          unless @stats.enabled_repetition_host_in_routing
            hosts = []
            nids.each{|nid| hosts << nid.split(/[:_]/)[0]}
            buf.delete_if{|nid| hosts.include?(nid.split(/[:_]/)[0])}
          else
            nids.each{|nid| buf.delete(nid) }
          end

          new_nid = buf[rand(buf.length)]
          new_nids = nids.map{|n| n == @stats.ap_str ? new_nid : n }
          unless sync_a_vnode_for_release(vn, new_nid, new_nids)
            @log.warn("release_process:error at hname=#{hname} vn=#{vn}")
          end
        end
      }
      @log.info("Release process has done.")
    rescue =>e
      @log.error("#{e}\n#{$@}")
    ensure
      @do_release_process = false
      Roma::Messaging::ConPool.instance.close_all
    end
 
    def sync_a_vnode_for_release(vn, to_nid, new_nids)
      if @stats.run_iterate_storage == true
        @log.warn("sync_a_vnode:already in being.#{vn} #{to_nid}")
        return false
      end
      nids = @rttable.search_nodes(vn)
      
      if nids.include?(to_nid)==false || (is_primary && nids[0]!=to_nid)
@log.debug("sync_a_vnode_for_release:#{vn} #{to_nid}")
        # change routing data at the vnode and synchronize a data
        nids << to_nid
        return false unless @rttable.transaction(vn, nids)

        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)

          if res != "STORED"
            @rttable.rollback(vn)
            @log.error("push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        }

        if (clk = @rttable.commit(vn)) == false
          @rttable.rollback(vn)
          @log.error("sync_a_vnode:routing table commit failed")
          return false
        end

        clk = @rttable.set_route(vn, clk, new_nids)
        if clk.is_a?(Integer) == false
          clk,new_nids = @rttable.search_nodes_with_clk(vn)
        end
        
        cmd = "setroute #{vn} #{clk - 1}"
        new_nids.each{ |nn| cmd << " #{nn}"}
        res = async_broadcast_cmd("#{cmd}\r\n")
@log.debug("async_a_vnode_for_release:async_broadcast_cmd(#{cmd}) #{res}")
      end

      return true
    rescue =>e
      @log.error("#{e}\n#{$@}")
      false
    end

    def sync_a_vnode(vn, to_nid, is_primary=nil)
      if @stats.run_iterate_storage == true
        @log.warn("sync_a_vnode:already in being.#{vn} #{to_nid} #{is_primary}")
        return false
      end
      nids = @rttable.search_nodes(vn)
      
      if nids.include?(to_nid)==false || (is_primary && nids[0]!=to_nid)
@log.debug("sync_a_vnode:#{vn} #{to_nid} #{is_primary}")
        # change routing data at the vnode and synchronize a data
        nids << to_nid
        return false unless @rttable.transaction(vn, nids)

        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)

          if res != "STORED"
            @rttable.rollback(vn)
            @log.error("push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        }

        if (clk = @rttable.commit(vn)) == false
          @rttable.rollback(vn)
          @log.error("sync_a_vnode:routing table commit failed")
          return false
        end

        nids = edit_nodes(nids, to_nid, is_primary)
        clk = @rttable.set_route(vn, clk, nids)
        if clk.is_a?(Integer) == false
          clk,nids = @rttable.search_nodes_with_clk(vn)
        end
        
        cmd = "setroute #{vn} #{clk - 1}"
        nids.each{ |nn| cmd << " #{nn}"}
        res = async_broadcast_cmd("#{cmd}\r\n")
@log.debug("sync_a_vnode:async_broadcast_cmd(#{cmd}) #{res}")
      else
        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)
          if res != "STORED"
            @log.error("push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        }
      end

      return true
    rescue =>e
      @log.error("#{e}\n#{$@}")
      false
    end

    def edit_nodes(nodes, new_nid, is_primary)
      if @rttable.rn == 1
        return [new_nid]
      end
      if nodes.length > @rttable.rn
        nodes.delete(new_nid)
        nodes.delete(nodes.last)
        nodes << new_nid
      end
      if is_primary
        nodes.delete(new_nid)
        nodes.insert(0,new_nid)
      end
      nodes
    end

    def push_a_vnode(hname, vn, nid)
      dmp = @storages[hname].dump(vn)
      unless dmp
        @log.info("hname=#{hname} vn=#{vn} has a empty data.")
        return "STORED"
      end
      con = Roma::Messaging::ConPool.instance.get_connection(nid)

      con.write("pushv #{hname} #{vn}\r\n")
      res = con.gets # READY\r\n or error string
      if res != "READY\r\n"
        con.close
        return res.chomp
      end
      con.write("#{dmp.length}\r\n#{dmp}\r\nEND\r\n")
      res = con.gets # STORED\r\n or error string

      Roma::Messaging::ConPool.instance.return_connection(nid,con)
      res.chomp! if res
      res
    rescue Errno::EPIPE
      @log.debug("Errno::EPIPE retry")
      retry
    rescue =>e
      @log.error("#{e.inspect}\n#{$@}")
      "#{e}"
    end
 
    def push_a_vnode_stream(hname, vn, nid)
      @stats.run_iterate_storage = true
      @log.info("push_a_vnode_stream:hname=#{hname} vn=#{vn} nid=#{nid}")
      con = Roma::Messaging::ConPool.instance.get_connection(nid)

      con.write("spushv #{hname} #{vn}\r\n")

      res = con.gets # READY\r\n or error string
      if res != "READY\r\n"
        con.close
        return res.chomp
      end

      @storages[hname].each_vn_dump(vn){|data|
        @stats.run_iterate_storage = true
        con.write(data)
        sleep @stats.stream_copy_wait_param
      }
      con.write("\0"*20) # end of steram

      res = con.gets # STORED\r\n or error string
      Roma::Messaging::ConPool.instance.return_connection(nid,con)
      res.chomp! if res
      res
    rescue =>e
      @log.error("#{e}\n#{$@}")
      e.to_s
    ensure
      @stats.run_iterate_storage = false
    end


    def asyncev_start_storage_clean_up_process(args)
#      @log.info("#{__method__}")
      if @stats.run_storage_clean_up
        @log.error("#{__method__}:already in being")
        return
      end
      @stats.run_storage_clean_up = true
      Thread::new{
        begin
          storage_clean_up_process
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.run_storage_clean_up = false
        end
      }
    end

    def storage_clean_up_process
#      @log.info("#{__method__}:start")
      me = @stats.ap_str
      vnhash={}
      @rttable.each_vnode{|vn, nids|
        if nids.include?(me)
          if nids[0] == me
            vnhash[vn] = :primary
          else
            vnhash[vn] = :secondary
          end
        end
      }
      t = Time.now.to_i - Roma::Config::STORAGE_DELMARK_EXPTIME
      count = 0
      @storages.each_pair{|hname,st|
        st.each_clean_up(t, vnhash){|key, vn|
          count += 1
          @stats.out_count += 1
#          @log.debug("#{__method__}:key=#{key} vn=#{vn}")
          nodes = @rttable.search_nodes_for_write(vn)
          next if(nodes.length <= 1)
          nodes[1..-1].each{|nid|
            res = async_send_cmd(nid,"out #{key}\e#{hname} #{vn}\r\n")
            unless res
              @log.warn("send out command failed:#{key}\e#{hname} #{vn} -> #{nid}")
            end
#            @log.debug("#{__method__}:res=#{res}")
          }
        }
      }
      if count>0
        @log.info("#{__method__}:#{count} keys deleted.")
      end
    end


  end # module AsyncProcess

end # module Roma
