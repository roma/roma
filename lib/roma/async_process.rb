require 'thread'
require 'digest/sha1'
require "timeout"

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
    @@async_queue_latency = Queue.new

    def self.queue
      @@async_queue
    end

    def self.queue_latency
      @@async_queue_latency
    end

    def start_async_process
      @async_thread = Thread.new{
        async_process_loop
      }
      @async_thread[:name] = __method__

      @async_thread_latency = Thread.new{
        async_process_loop_for_latency
      }
      @async_thread_latency[:name] = __method__
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

      count = 0
      while @@async_queue_latency.empty? == false && count < 100
        count += 1
        sleep 0.1
      end
      @async_thread_latency.exit
    end

    def async_process_loop
      loop {
        while msg = @@async_queue.pop
          if send("asyncev_#{msg.event}",msg.args)
            msg.callback.call(msg,true) if msg.callback
          else
            if msg.retry?
              t = Thread.new{
                msg.wait
                msg.incr_count
                @@async_queue.push(msg)
              }
              t[:name] = __method__
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

    def async_process_loop_for_latency
      loop {
        while msg = @@async_queue_latency.pop
          if send("asyncev_#{msg.event}",msg.args)
            msg.callback.call(msg,true) if msg.callback
          else
            if msg.retry?
              t = Thread.new{
                msg.wait
                msg.incr_count
                @@async_queue_latency.push(msg)
              }
              t[:name] = __method__
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
      @log.debug("#{__method__} #{args.inspect}")
      cmd, nids, tout = args
      t = Thread::new{
        async_broadcast_cmd("#{cmd}\r\n", nids, tout)
      }
      t[:name] = __method__
      true
    end

    def asyncev_start_join_process(args)
      @log.debug(__method__)
      if @stats.run_join
        @log.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @log.error("#{__method__}:recover process running")
        return true
      end
      if @stats.run_balance
        @log.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_join = true
      t = Thread::new do
        begin
          join_process
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.run_join = false
          @stats.join_ap = nil
        end
      end
      t[:name] = __method__
      true
    end

    def asyncev_start_balance_process(args)
      @log.debug(__method__)
      if @stats.run_join
        @log.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @log.error("#{__method__}:recover process running")
        return true
      end
      if @stats.run_balance
        @log.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_balance = true
      t = Thread::new do
        begin
          balance_process
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.run_balance = false
        end
      end
      t[:name] = __method__
      true
    end

    def asyncev_redundant(args)
      nid,hname,k,d,clk,expt,v = args
      @log.debug("#{__method__} #{args.inspect}")
      unless @rttable.nodes.include?(nid)
        @log.warn("async redundant failed:#{nid} dose not found in routing table.#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}")
        return true # no retry
      end
      res = async_send_cmd(nid,"rset #{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}\r\n#{v}\r\n",10)
      if res == nil || res.start_with?("ERROR")
        @log.warn("async redundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_zredundant(args)
      nid,hname,k,d,clk,expt,zv = args
      @log.debug("#{__method__} #{args.inspect}")
      unless @rttable.nodes.include?(nid)
        @log.warn("async zredundant failed:#{nid} dose not found in routing table.#{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}")
        return true # no retry
      end
      res = async_send_cmd(nid,"rzset #{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}\r\n#{zv}\r\n",10)
      if res == nil || res.start_with?("ERROR")
        @log.warn("async zredundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_rdelete(args)
      nid,hname,k,clk = args
      @log.debug("#{__method__} #{args.inspect}")
      unless @rttable.nodes.include?(nid)
        @log.warn("async rdelete failed:#{nid} dose not found in routing table.#{k}\e#{hname} #{clk}")
        return true # no retry
      end
      res = async_send_cmd(nid,"rdelete #{k}\e#{hname} #{clk}\r\n",10)
      unless res
        @log.warn("async redundant failed:#{k}\e#{hname} #{clk} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_reqpushv(args)
      vn, nid, p = args
      @log.debug("#{__method__} #{args.inspect}")
      if @stats.run_iterate_storage
        @log.warn("#{__method__}:already be iterated storage process.")
      else
        @stats.run_iterate_storage = true
        t = Thread::new do
          begin
            sync_a_vnode(vn.to_i, nid, p == 'true')
          rescue =>e
            @log.error("#{__method__}:#{e.inspect} #{$@}")
          ensure
            @stats.run_iterate_storage = false
          end
        end
        t[:name] = __method__
      end
    end

    def asyncev_start_recover_process(args)
      @log.debug("#{__method__} #{args.inspect}")
      if @stats.run_join
        @log.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @log.error("#{__method__}:recover process running.")
        return false
      end
      if @stats.run_balance
        @log.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_recover = true
      t = Thread::new do
        begin
          acquired_recover_process
        rescue => e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.run_recover = false
        end
      end
      t[:name] = __method__
    end

    def asyncev_start_auto_recover_process(args)
      @log.debug("#{__method__} #{args.inspect}")
      ###run_join don't have possibility to be true in this case.
      #if @stats.run_join
      #  @log.error("#{__method__}:join process running")
      #  return true
      #end
      if @stats.run_recover
        @log.error("#{__method__}:recover process running.")
        return false
      end
      if @stats.run_balance
        @log.error("#{__method__}:balance process running")
        return true
      end

      @rttable.auto_recover_status = "preparing"
      t = Thread::new do
        begin
          timeout(@rttable.auto_recover_time){
            loop{
              sleep 1
              break if @rttable.auto_recover_status != "preparing"
              #break if @stats.run_join #run_join don't have possibility to be true in this case.
              break if @stats.run_recover
              break if @stats.run_balance
            }
          }
          @log.debug("inactivated AUTO_RECOVER")
        rescue
          case @rttable.lost_action
            when :auto_assign, :shutdown
              @stats.run_recover = true
              @rttable.auto_recover_status = "executing"
                begin
                  @log.debug("auto recover start")
                  acquired_recover_process
                rescue => e
                  @log.error("#{__method__}:#{e.inspect} #{$@}")
                ensure
                  @stats.run_recover = false
                  @rttable.auto_recover_status = "waiting"
                end
            when :no_action
              @log.debug("auto recover NOT start. Because lost action is [no_action]")
          end
        end
      end
      t[:name] = __method__
    end

    def asyncev_start_release_process(args)
      @log.debug("#{__method__} #{args}")
      if @stats.run_iterate_storage
        @log.warn("#{__method__}:already be iterated storage process.")
      else
        @stats.run_release = true
        @stats.run_iterate_storage = true
        @stats.spushv_protection = true
        t = Thread::new do
          begin
            release_process
          rescue => e
            @log.error("#{__method__}:#{e.inspect} #{$@}")
          ensure
            @stats.run_iterate_storage = false
            @stats.run_release = false
          end
        end
        t[:name] = __method__
      end
    end

    def acquired_recover_process
      @log.info("#{__method__}:start")

      exclude_nodes = @rttable.exclude_nodes_for_recover(@stats.ap_str, @stats.rep_host)

      @do_acquired_recover_process = true
      loop do
        break unless @do_acquired_recover_process
        break if @rttable.num_of_vn(@stats.ap_str)[2] == 0 # short vnodes

        vn, nodes, is_primary = @rttable.select_vn_for_recover(exclude_nodes)
        break unless vn

        if nodes.length != 0
          ret = req_push_a_vnode(vn, nodes[0], is_primary)
          if ret == :rejected
            sleep 1
          elsif ret == false
            break
          end
          sleep 1
        end
      end
      @log.info("#{__method__} has done.")
    rescue => e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @do_acquired_recover_process = false
    end

    def join_process
      @log.info("#{__method__}:start")
      count = 0
      nv = @rttable.v_idx.length
      exclude_nodes = @rttable.exclude_nodes_for_join(@stats.ap_str, @stats.rep_host)

      @do_join_process = true
      while (@rttable.vnode_balance(@stats.ap_str) == :less && count < nv) do
        break unless @do_join_process

        vn, nodes, is_primary = @rttable.select_vn_for_join(exclude_nodes)
        unless vn
          @log.warn("#{__method__}:vnode dose not found")
          return false
        end
        ret = req_push_a_vnode(vn, nodes[0], is_primary)
        if ret == :rejected
          sleep 5
        else
          sleep 1
          count += 1
        end
      end
    rescue => e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @log.info("#{__method__} has done.")
      @do_join_process = false
    end

    def balance_process
      @log.info("#{__method__}:start")
      count = 0
      nv = @rttable.v_idx.length
      exclude_nodes = @rttable.exclude_nodes_for_balance(@stats.ap_str, @stats.rep_host)

      @do_balance_process = true
      while (@rttable.vnode_balance(@stats.ap_str) == :less && count < nv) do
        break unless @do_balance_process

        vn, nodes, is_primary = @rttable.select_vn_for_balance(exclude_nodes)
        unless vn
          @log.warn("#{__method__}:vnode dose not found")
          return false
        end
        ret = req_push_a_vnode(vn, nodes[0], is_primary)
        if ret == :rejected
          sleep 5
        else
          sleep 1
          count += 1
        end
      end
      @log.info("#{__method__} has done.")
    rescue => e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @do_balance_process = false
    end

    def req_push_a_vnode(vn, src_nid, is_primary)
      con = Roma::Messaging::ConPool.instance.get_connection(src_nid)
      con.write("reqpushv #{vn} #{@stats.ap_str} #{is_primary}\r\n")
      res = con.gets # receive 'PUSHED\r\n' | 'REJECTED\r\n' | 'ERROR\r\n'
      if res == "REJECTED\r\n"
        @log.warn("#{__method__}:request was rejected from #{src_nid}.")
        Roma::Messaging::ConPool.instance.return_connection(src_nid,con)
        return :rejected
      elsif res != "PUSHED\r\n"
        @log.warn("#{__method__}:#{res}")
        return :rejected
      end
      Roma::Messaging::ConPool.instance.return_connection(src_nid,con)
      # waiting for pushv
      count = 0
      while @rttable.search_nodes(vn).include?(@stats.ap_str)==false && count < @stats.reqpushv_timeout_count
        sleep 0.1
        count += 1
      end
      if count >= @stats.reqpushv_timeout_count
        @log.warn("#{__method__}:request has been time-out.vn=#{vn} nid=#{src_nid}")
        return :timeout
      end
      true
    rescue =>e
      @log.error("#{__method__}:#{e.inspect} #{$@}")
      @rttable.proc_failed(src_nid)
      false
    end

    def release_process
      @log.info("#{__method__}:start.")

      if @rttable.can_i_release?(@stats.ap_str, @stats.rep_host)
        @log.error("#{__method__}:Sufficient nodes do not found.")
        return
      end

      @do_release_process = true
      while(@rttable.has_node?(@stats.ap_str)) do
        break unless @do_release_process
        @rttable.each_vnode do |vn, nids|
          break unless @do_release_process
          if nids.include?(@stats.ap_str)

            to_nid, new_nids = @rttable.select_node_for_release(@stats.ap_str, @stats.rep_host, nids)
            res = sync_a_vnode_for_release(vn, to_nid, new_nids)
            if res == :abort
              @log.error("#{__method__}:release_process aborted due to SERVER_ERROR received.")
              @do_release_process = false
            end
            if res == false
              @log.warn("#{__method__}:error at vn=#{vn} to_nid=#{to_nid} new_nid=#{new_nids}")
              redo
            end
          end
        end
      end
      @log.info("#{__method__} has done.")
    rescue =>e
      @log.error("#{e}\n#{$@}")
    ensure
      @do_release_process = false
      Roma::Messaging::ConPool.instance.close_all
    end

    def sync_a_vnode_for_release(vn, to_nid, new_nids)
      nids = @rttable.search_nodes(vn)

      if nids.include?(to_nid)==false
        @log.debug("#{__method__}:#{vn} #{to_nid}")
        # change routing data at the vnode and synchronize a data
        nids << to_nid
        return false unless @rttable.transaction(vn, nids)

        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)

          if res != "STORED"
            @rttable.rollback(vn)
            @log.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return :abort if res.start_with?("SERVER_ERROR")
            return false
          end
        }

        if (clk = @rttable.commit(vn)) == false
          @rttable.rollback(vn)
          @log.error("#{__method__}:routing table commit failed")
          return false
        end

        clk = @rttable.set_route(vn, clk, new_nids)
        if clk.is_a?(Integer) == false
          clk,new_nids = @rttable.search_nodes_with_clk(vn)
        end

        cmd = "setroute #{vn} #{clk - 1}"
        new_nids.each{ |nn| cmd << " #{nn}"}
        res = async_broadcast_cmd("#{cmd}\r\n")
        @log.debug("#{__method__}:async_broadcast_cmd(#{cmd}) #{res}")
      end

      return true
    rescue =>e
      @log.error("#{e}\n#{$@}")
      false
    end

    def sync_a_vnode(vn, to_nid, is_primary=nil)
      nids = @rttable.search_nodes(vn)

      if nids.include?(to_nid)==false || (is_primary && nids[0]!=to_nid)
        @log.debug("#{__method__}:#{vn} #{to_nid} #{is_primary}")
        # change routing data at the vnode and synchronize a data
        nids << to_nid
        return false unless @rttable.transaction(vn, nids)

        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)

          if res != "STORED"
            @rttable.rollback(vn)
            @log.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        }

        if (clk = @rttable.commit(vn)) == false
          @rttable.rollback(vn)
          @log.error("#{__method__}:routing table commit failed")
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
        @log.debug("#{__method__}:async_broadcast_cmd(#{cmd}) #{res}")
      else
        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)
          if res != "STORED"
            @log.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
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
      # [node_a, node_b, new_nid]
      nodes.delete(new_nid)
      # [node_a, node_b]

      if nodes.length >= @rttable.rn
        host = new_nid.split(/[:_]/)[0]
        buf = [] # list of a same host
        nodes.each do |nid|
          buf << nid if nid.split(/[:_]/)[0] == host
        end
        if buf.length > 0
          # include same host
          # delete a last one, due to save a primary node
          nodes.delete(buf.last)
        else
          nodes.delete(nodes.last)
        end
      end

      if is_primary
        # [new_nid, node_a]
        nodes.insert(0,new_nid)
      else
        # [node_a, new_nid]
        nodes << new_nid
      end
      nodes
    end

    def push_a_vnode_stream(hname, vn, nid)
      @log.info("#{__method__}:hname=#{hname} vn=#{vn} nid=#{nid}")

      stop_clean_up

      con = Roma::Messaging::ConPool.instance.get_connection(nid)

      @do_push_a_vnode_stream = true

      con.write("spushv #{hname} #{vn}\r\n")

      res = con.gets # READY\r\n or error string
      if res != "READY\r\n"
        con.close
        return res.chomp
      end

      res_dump = @storages[hname].each_vn_dump(vn) do |data|

        unless @do_push_a_vnode_stream
          con.close
          @log.error("#{__method__}:canceled in hname=#{hname} vn=#{vn} nid=#{nid}")
          return "CANCELED"
        end

        con.write(data)
        sleep @stats.stream_copy_wait_param
      end
      con.write("\0"*20) # end of steram

      res = con.gets # STORED\r\n or error string
      Roma::Messaging::ConPool.instance.return_connection(nid,con)
      res.chomp! if res
      if res_dump == false
        @log.error("#{__method__}:each_vn_dump in hname=#{hname} vn=#{vn} nid=#{nid}")
        return "CANCELED"
      end
      res
    rescue =>e
      @log.error("#{e}\n#{$@}")
      e.to_s
    end


    def asyncev_start_storage_clean_up_process(args)
#      @log.info("#{__method__}")
      if @stats.run_storage_clean_up
        @log.error("#{__method__}:already in being")
        return
      end
      @stats.run_storage_clean_up = true
      t = Thread::new{
        begin
          storage_clean_up_process
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.last_clean_up = Time.now
          @stats.run_storage_clean_up = false
        end
      }
      t[:name] = __method__
    end

    def storage_clean_up_process
      @log.info("#{__method__}:start")
      me = @stats.ap_str
      vnhash={}
      @rttable.each_vnode do |vn, nids|
        if nids.include?(me)
          if nids[0] == me
            vnhash[vn] = :primary
          else
            vnhash[vn] = :secondary
          end
        end
      end
      t = Time.now.to_i - Roma::Config::STORAGE_DELMARK_EXPTIME
      count = 0
      @storages.each_pair do |hname,st|
        break unless @stats.do_clean_up?
        st.each_clean_up(t, vnhash) do |key, vn|
          # @log.debug("#{__method__}:key=#{key} vn=#{vn}")
          if @stats.run_receive_a_vnode.key?("#{hname}_#{vn}")
            false
          else
            nodes = @rttable.search_nodes_for_write(vn)
            if nodes && nodes.length > 1
              nodes[1..-1].each do |nid|
                res = async_send_cmd(nid,"out #{key}\e#{hname} #{vn}\r\n")
                unless res
                  @log.warn("send out command failed:#{key}\e#{hname} #{vn} -> #{nid}")
                end
                # @log.debug("#{__method__}:res=#{res}")
              end
            end
            count += 1
            @stats.out_count += 1
            true
          end
        end
      end
      if count>0
        @log.info("#{__method__}:#{count} keys deleted.")
      end

      # delete @rttable.logs
      if @stats.gui_run_gather_logs || @rttable.logs.empty?
        false
      else
        gathered_time = @rttable.logs[0]
        # delete gathering log data after 5min
        @rttable.logs.clear if gathered_time.to_i < Time.now.to_i - (60 * 5)
      end
    ensure
      @log.info("#{__method__}:stop")
    end

    def asyncev_calc_latency_average(args)
      latency,cmd = args
      #@log.debug(__method__)

      if !@stats.latency_data.key?(cmd) #only first execute target cmd
        @stats.latency_data[cmd].store("latency", Array.new())
        @stats.latency_data[cmd].store("latency_max", Hash.new())
        @stats.latency_data[cmd]["latency_max"].store("current", 0)
        @stats.latency_data[cmd].store("latency_min", Hash.new())
        @stats.latency_data[cmd]["latency_min"].store("current", 99999)
        @stats.latency_data[cmd].store("time", Time.now.to_i)
      end

      begin
        @stats.latency_data[cmd]["latency"].delete_at(0) if @stats.latency_data[cmd]["latency"].length >= 10
        @stats.latency_data[cmd]["latency"].push(latency)

        @stats.latency_data[cmd]["latency_max"]["current"] = latency if latency > @stats.latency_data[cmd]["latency_max"]["current"]
        @stats.latency_data[cmd]["latency_min"]["current"] = latency if latency < @stats.latency_data[cmd]["latency_min"]["current"]

      rescue =>e
        @log.error("#{__method__}:#{e.inspect} #{$@}")

      ensure
        if @stats.latency_check_time_count && Time.now.to_i - @stats.latency_data[cmd]["time"] > @stats.latency_check_time_count
          average = @stats.latency_data[cmd]["latency"].inject(0.0){|r,i| r+=i }/@stats.latency_data[cmd]["latency"].size
          max = @stats.latency_data[cmd]["latency_max"]["current"]
          min = @stats.latency_data[cmd]["latency_min"]["current"]
          @log.debug("Latency average[#{cmd}]: #{sprintf("%.8f", average)}"+
                     "(denominator=#{@stats.latency_data[cmd]["latency"].length}"+
                     " max=#{sprintf("%.8f", max)}"+
                     " min=#{sprintf("%.8f", min)})"
                    )

          @stats.latency_data[cmd]["time"] =  Time.now.to_i
          @stats.latency_data[cmd]["latency_past"] = @stats.latency_data[cmd]["latency"]
          @stats.latency_data[cmd]["latency"] = []
          @stats.latency_data[cmd]["latency_max"]["past"] = @stats.latency_data[cmd]["latency_max"]["current"]
          @stats.latency_data[cmd]["latency_max"]["current"] = 0
          @stats.latency_data[cmd]["latency_min"]["past"] = @stats.latency_data[cmd]["latency_min"]["current"]
          @stats.latency_data[cmd]["latency_min"]["current"] = 99999
        end
      end
      true
    end

    def asyncev_start_storage_flush_process(args)
      hname, dn = args
      @log.debug("#{__method__} #{args.inspect}")

      st = @storages[hname]
      if st.dbs[dn] != :safecopy_flushing
        @log.error("Can not flush storage. stat = #{st.dbs[dn]}")
        return true
      end
      t = Thread::new do
        begin
          st.flush_db(dn)
          st.set_db_stat(dn,:safecopy_flushed)
          @log.info("#{__method__}:storage has flushed. (#{hname}, #{dn})")
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
        end
      end
      t[:name] = __method__
      true
    end

    def asyncev_start_storage_cachecleaning_process(args)
      hname, dn = args
      @log.debug("#{__method__} #{args.inspect}")

      st = @storages[hname]
      if st.dbs[dn] != :cachecleaning
        @log.error("Can not start cachecleaning process. stat = #{st.dbs[dn]}")
        return true
      end
      t = Thread::new do
        begin
          storage_cachecleaning_process(hname, dn)
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
        end
      end
      t[:name] = __method__
      true
    end

    def storage_cachecleaning_process(hname, dn)
      count = 0
      rcount = 0
      st = @storages[hname]

      @do_storage_cachecleaning_process = true
      loop do
        # get keys in a cache up to 100 kyes
        keys = st.get_keys_in_cache(dn)
        break if keys == nil || keys.length == 0
        break unless @do_storage_cachecleaning_process

        # @log.debug("#{__method__}:#{keys.length} keys found")

        # copy cache -> db
        st.each_cache_by_keys(dn, keys) do |vn, last, clk, expt, k, v|
          break unless @do_storage_cachecleaning_process
          if st.load_stream_dump_for_cachecleaning(vn, last, clk, expt, k, v)
            count += 1
            # @log.debug("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was stored.")
          else
            rcount += 1
            # @log.debug("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was rejected.")
          end
        end

        # remove keys in a cache
        keys.each { |key| st.out_cache(dn, key) }
      end
      if @do_storage_cachecleaning_process == false
        @log.warn("#{__method__}:uncompleted")
      else
        st.set_db_stat(dn,:normal)
      end
      @log.debug("#{__method__}:#{count} keys loaded.")
      @log.debug("#{__method__}:#{rcount} keys rejected.") if rcount > 0
    ensure
      @do_storage_cachecleaning_process = false
    end

    def asyncev_start_get_routing_event(args)
      @log.debug("#{__method__} #{args}")
      t = Thread::new do
        begin
          get_routing_event
        rescue => e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
        end
      end
      t[:name] = __method__
    end

    def get_routing_event
      @log.info("#{__method__}:start.")

      routing_path = Config::RTTABLE_PATH
      f_list = Dir.glob("#{routing_path}/#{@stats.ap_str}*")

      f_list.each{|fname|
        IO.foreach(fname){|line|
          if line =~ /join|leave/
            @rttable.event.shift if @rttable.event.size >= @rttable.event_limit_line
            @rttable.event << line.chomp 
          end
        }
      }

      @log.info("#{__method__} has done.")
    rescue =>e
      @log.error("#{e}\n#{$@}")
    end

    def asyncev_start_get_logs(args)
      @log.debug("#{__method__} #{args}")
      t = Thread::new do
        begin
          get_logs(args)
        rescue => e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.gui_run_gather_logs = false
        end
      end
      t[:name] = __method__
    end

    def get_logs(args)
      @log.debug("#{__method__}:start.")

      log_path =  Config::LOG_PATH
      log_file = "#{log_path}/#{@stats.ap_str}.log"

      target_logs = []
      File.open(log_file){|f|
        start_point = get_point(f, args[0], 'start')
        end_point = get_point(f, args[1], 'end')

        ## read target logs
        f.seek(start_point, IO::SEEK_SET)
        target_logs = f.read(end_point - start_point)
        target_logs = target_logs.each_line.map(&:chomp)
        # cut untarget lines
        target_logs.each_with_index{ |log, index|
          if log =~ /[IDEW],\s\[#{args[0]}/
            target_logs.slice!(0, index)
            break
          end
        }
        target_logs.each_with_index{ |log, index|
          if log =~ /[IDEW],\s\[#{args[1]}/
            target_logs.slice!(index..-1)
            break
          end
        }
        target_logs.delete('.')
      }

      @rttable.logs = target_logs
      # set gathered date for expiration
      @rttable.logs.unshift(Time.now)

      @log.debug("#{__method__} has done.")
    rescue =>e
      @rttable.logs = []
      @log.error("#{e}\n#{$@}")
    ensure
      @stats.gui_run_gather_logs = false
    end

    def get_point(f, target_time, type, c_pos=0, n_pos=f.size/2)
      # initialize read size
      read_size = 2048

      # first check
      unless target_time.class == Time
        # in case of not set end_date
        return f.size if target_time == 'current'

        target_time =~ (/(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)/)
        target_time = Time.mktime($1, $2, $3, $4, $5, $6, 000000)
 
        # check outrange or not
        f.seek(0, IO::SEEK_SET)
        begining_log = f.read(read_size)
        pos = begining_log.index(/[IDEW],\s\[(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)\.(\d+)/)
        begining_time = Time.mktime($1, $2, $3, $4, $5, $6, $7)

        f.seek(-read_size, IO::SEEK_END)
        end_log = f.read(read_size)
        pos = end_log.rindex(/[IDEW],\s\[(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)\.(\d+)/)
        end_time = Time.mktime($1, $2, $3, $4, $5, $6, $7)

        case type
        when 'start'
          if target_time < begining_time
            return 0
          elsif target_time > end_time
            @log.error("irregular time was set.")
            raise
          end
        when 'end'
          if target_time > end_time
            return f.size
          elsif target_time < begining_time
            @log.error("irregular time was set.")
            raise
          end
        end
      end

      # read half sector size
      f.seek(n_pos, IO::SEEK_SET)
      sector_log = f.read(read_size)
      # grep date
      date_a = sector_log.scan(/[IDEW],\s\[(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)\.(\d+)/)
      sector_time_first = Time.mktime(date_a[0][0], date_a[0][1], date_a[0][2], date_a[0][3], date_a[0][4], date_a[0][5], date_a[0][6])
      sector_time_last = Time.mktime(date_a[-1][0], date_a[-1][1], date_a[-1][2], date_a[-1][3], date_a[-1][4], date_a[-1][5], date_a[-1][6])
      
      # compare time
      #start_time = Time.now
      #loop do
      ## hilatency check
      #ps = Time.now - start_time
      #if ps > 5
      #  @log.warn("gather_logs process was failed.")
      #  raise
      #end

      if target_time.between?(sector_time_first, sector_time_last)
        if type == 'start'
          return n_pos
        elsif type == 'end'
          return f.pos
        end
      elsif sector_time_first > target_time
        t_pos = n_pos - ((n_pos - c_pos).abs / 2)
      elsif sector_time_first < target_time
        t_pos = n_pos + ((n_pos - c_pos).abs / 2)
      end
      
      get_point(f, target_time, type, n_pos, t_pos)
    end

  end # module AsyncProcess

end # module Roma
