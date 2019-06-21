require 'thread'
require 'digest/sha1'
require 'timeout'

module Roma
  class AsyncMessage
    attr_accessor :event
    attr_accessor :args
    attr_accessor :callback

    def initialize(ev, ag = nil, &cb)
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
      @async_thread = Thread.new do
        async_process_loop
      end
      @async_thread[:name] = __method__

      @async_thread_latency = Thread.new do
        async_process_loop_for_latency
      end
      @async_thread_latency[:name] = __method__
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
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
      loop do
        while msg = @@async_queue.pop
          if send("asyncev_#{msg.event}", msg.args)
            msg.callback.call(msg, true) if msg.callback
          else
            if msg.retry?
              t = Thread.new do
                msg.wait
                msg.incr_count
                @@async_queue.push(msg)
              end
              t[:name] = __method__
            else
              @logger.error("async process retry out:#{msg.inspect}")
              msg.callback.call(msg, false) if msg.callback
            end
          end
        end
      end
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
      retry
    end

    def async_process_loop_for_latency
      loop do
        while msg = @@async_queue_latency.pop
          if send("asyncev_#{msg.event}", msg.args)
            msg.callback.call(msg, true) if msg.callback
          else
            if msg.retry?
              t = Thread.new do
                msg.wait
                msg.incr_count
                @@async_queue_latency.push(msg)
              end
              t[:name] = __method__
            else
              @logger.error("async process retry out:#{msg.inspect}")
              msg.callback.call(msg, false) if msg.callback
            end
          end
        end
      end
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
      retry
    end

    def asyncev_broadcast_cmd(args)
      @logger.debug("#{__method__} #{args.inspect}")
      cmd, nids, tout = args
      t = Thread.new do
        async_broadcast_cmd("#{cmd}\r\n", nids, tout)
      end
      t[:name] = __method__
      true
    end

    def asyncev_start_join_process(_args)
      @logger.debug(__method__)
      if @stats.run_join
        @logger.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @logger.error("#{__method__}:recover process running")
        return true
      end
      if @stats.run_balance
        @logger.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_join = true
      t = Thread.new do
        begin
          join_process
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
        ensure
          @stats.run_join = false
          @stats.join_ap = nil
        end
      end
      t[:name] = __method__
      true
    end

    def asyncev_start_balance_process(_args)
      @logger.debug(__method__)
      if @stats.run_join
        @logger.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @logger.error("#{__method__}:recover process running")
        return true
      end
      if @stats.run_balance
        @logger.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_balance = true
      t = Thread.new do
        begin
          balance_process
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
        ensure
          @stats.run_balance = false
        end
      end
      t[:name] = __method__
      true
    end

    def asyncev_redundant(args)
      nid, hname, k, d, clk, expt, v = args
      @logger.debug("#{__method__} #{args.inspect}")
      unless @routing_table.nodes.include?(nid)
        @logger.warn("async redundant failed:#{nid} does not found in routing table.#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}")
        return true # no retry
      end
      res = async_send_cmd(nid, "rset #{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}\r\n#{v}\r\n", 10)
      if res.nil? || res.start_with?('ERROR')
        @logger.warn("async redundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_zredundant(args)
      nid, hname, k, d, clk, expt, zv = args
      @logger.debug("#{__method__} #{args.inspect}")
      unless @routing_table.nodes.include?(nid)
        @logger.warn("async zredundant failed:#{nid} does not found in routing table.#{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}")
        return true # no retry
      end
      res = async_send_cmd(nid, "rzset #{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}\r\n#{zv}\r\n", 10)
      if res.nil? || res.start_with?('ERROR')
        @logger.warn("async zredundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_rdelete(args)
      nid, hname, k, clk = args
      @logger.debug("#{__method__} #{args.inspect}")
      unless @routing_table.nodes.include?(nid)
        @logger.warn("async rdelete failed:#{nid} does not found in routing table.#{k}\e#{hname} #{clk}")
        return true # no retry
      end
      res = async_send_cmd(nid, "rdelete #{k}\e#{hname} #{clk}\r\n", 10)
      unless res
        @logger.warn("async redundant failed:#{k}\e#{hname} #{clk} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_reqpushv(args)
      vn, nid, p = args
      @logger.debug("#{__method__} #{args.inspect}")
      if @stats.run_iterate_storage
        @logger.warn("#{__method__}:already be iterated storage process.")
      else
        @stats.run_iterate_storage = true
        t = Thread.new do
          begin
            sync_a_vnode(vn.to_i, nid, p == 'true')
          rescue => e
            @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
          ensure
            @stats.run_iterate_storage = false
          end
        end
        t[:name] = __method__
      end
    end

    def asyncev_start_recover_process(args)
      @logger.debug("#{__method__} #{args.inspect}")
      if @stats.run_join
        @logger.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @logger.error("#{__method__}:recover process running.")
        return false
      end
      if @stats.run_balance
        @logger.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_recover = true
      t = Thread.new do
        begin
          acquired_recover_process
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
        ensure
          @stats.run_recover = false
        end
      end
      t[:name] = __method__
    end

    def asyncev_start_auto_recover_process(args)
      @logger.debug("#{__method__} #{args.inspect}")
      # ##run_join don't have possibility to be true in this case.
      # if @stats.run_join
      #  @logger.error("#{__method__}:join process running")
      #  return true
      # end
      if @stats.run_recover
        @logger.error("#{__method__}:recover process running.")
        return false
      end
      if @stats.run_balance
        @logger.error("#{__method__}:balance process running")
        return true
      end

      @routing_table.auto_recover_status = 'preparing'
      t = Thread.new do
        begin
          Timeout.timeout(@routing_table.auto_recover_time)do
            loop do
              sleep 1
              break if @routing_table.auto_recover_status != 'preparing'
              # break if @stats.run_join #run_join don't have possibility to be true in this case.
              break if @stats.run_recover
              break if @stats.run_balance
            end
          end
          @logger.debug('inactivated AUTO_RECOVER')
        rescue
          case @routing_table.lost_action
            when :auto_assign, :shutdown
              @stats.run_recover = true
              @routing_table.auto_recover_status = 'executing'
              begin
                @logger.debug('auto recover start')
                acquired_recover_process
              rescue => e
                @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
              ensure
                @stats.run_recover = false
                @routing_table.auto_recover_status = 'waiting'
              end
            when :no_action
              @logger.debug('auto recover NOT start. Because lost action is [no_action]')
          end
        end
      end
      t[:name] = __method__
    end

    def asyncev_start_release_process(args)
      @logger.debug("#{__method__} #{args}")
      if @stats.run_iterate_storage
        @logger.warn("#{__method__}:already be iterated storage process.")
      else
        @stats.run_release = true
        @stats.run_iterate_storage = true
        @stats.spushv_protection = true
        t = Thread.new do
          begin
            release_process
          rescue => e
            @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
          ensure
            @stats.run_iterate_storage = false
            @stats.run_release = false
          end
        end
        t[:name] = __method__
      end
    end

    def acquired_recover_process
      @logger.info("#{__method__}:start")

      exclude_nodes = @routing_table.exclude_nodes_for_recover(@stats.ap_str, @stats.rep_host)

      @do_acquired_recover_process = true
      loop do
        break unless @do_acquired_recover_process
        break if @routing_table.num_of_vn(@stats.ap_str)[2] == 0 # short vnodes

        vn, nodes, is_primary = @routing_table.select_vn_for_recover(exclude_nodes, @stats.rep_host)
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
      @logger.info("#{__method__} has done.")
    rescue => e
      @logger.error("#{e.inspect} #{$ERROR_POSITION}")
    ensure
      @do_acquired_recover_process = false
    end

    def join_process
      @logger.info("#{__method__}:start")
      count = 0
      nv = @routing_table.v_idx.length
      exclude_nodes = @routing_table.exclude_nodes_for_join(@stats.ap_str, @stats.rep_host)

      @do_join_process = true
      while @routing_table.vnode_balance(@stats.ap_str) == :less && count < nv
        break unless @do_join_process

        vn, nodes, is_primary = @routing_table.select_vn_for_join(exclude_nodes, @stats.rep_host)
        unless vn
          @logger.warn("#{__method__}:vnode does not found")
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
      @logger.error("#{e.inspect} #{$ERROR_POSITION}")
    ensure
      @logger.info("#{__method__} has done.")
      @do_join_process = false
    end

    def balance_process
      @logger.info("#{__method__}:start")
      count = 0
      nv = @routing_table.v_idx.length
      exclude_nodes = @routing_table.exclude_nodes_for_balance(@stats.ap_str, @stats.rep_host)

      @do_balance_process = true
      while @routing_table.vnode_balance(@stats.ap_str) == :less && count < nv
        break unless @do_balance_process

        vn, nodes, is_primary = @routing_table.select_vn_for_balance(exclude_nodes, @stats.rep_host)
        unless vn
          @logger.warn("#{__method__}:vnode does not found")
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
      @logger.info("#{__method__} has done.")
    rescue => e
      @logger.error("#{e.inspect} #{$ERROR_POSITION}")
    ensure
      @do_balance_process = false
    end

    def req_push_a_vnode(vn, src_nid, is_primary)
      con = Roma::Messaging::ConPool.instance.get_connection(src_nid)
      con.write("reqpushv #{vn} #{@stats.ap_str} #{is_primary}\r\n")
      res = con.gets # receive 'PUSHED\r\n' | 'REJECTED\r\n' | 'ERROR\r\n'
      if res == "REJECTED\r\n"
        @logger.warn("#{__method__}:request was rejected from #{src_nid}.")
        Roma::Messaging::ConPool.instance.return_connection(src_nid, con)
        return :rejected
      elsif res != "PUSHED\r\n"
        @logger.warn("#{__method__}:#{res}")
        return :rejected
      end
      Roma::Messaging::ConPool.instance.return_connection(src_nid, con)
      # waiting for pushv
      count = 0
      while @routing_table.search_nodes(vn).include?(@stats.ap_str) == false && count < @stats.reqpushv_timeout_count
        sleep 0.1
        count += 1
      end
      if count >= @stats.reqpushv_timeout_count
        @logger.warn("#{__method__}:request has been time-out.vn=#{vn} nid=#{src_nid}")
        return :timeout
      end
      true
    rescue => e
      @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
      @routing_table.proc_failed(src_nid)
      false
    end

    def release_process
      @logger.info("#{__method__}:start.")

      if @routing_table.can_i_release?(@stats.ap_str, @stats.rep_host)
        @logger.error("#{__method__}:Sufficient nodes do not found.")
        return
      end

      @do_release_process = true
      while @routing_table.has_node?(@stats.ap_str)
        break unless @do_release_process
        @routing_table.each_vnode do |vn, nids|
          break unless @do_release_process
          if nids.include?(@stats.ap_str)

            to_nid, new_nids = @routing_table.select_node_for_release(@stats.ap_str, @stats.rep_host, nids)
            res = sync_a_vnode_for_release(vn, to_nid, new_nids)
            if res == :abort
              @logger.error("#{__method__}:release_process aborted due to SERVER_ERROR received.")
              @do_release_process = false
            end
            if res == false
              @logger.warn("#{__method__}:error at vn=#{vn} to_nid=#{to_nid} new_nid=#{new_nids}")
              redo
            end
          end
        end
      end
      @logger.info("#{__method__} has done.")
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
    ensure
      @do_release_process = false
      Roma::Messaging::ConPool.instance.close_all
    end

    def sync_a_vnode_for_release(vn, to_nid, new_nids)
      nids = @routing_table.search_nodes(vn)

      if nids.include?(to_nid) == false
        @logger.debug("#{__method__}:#{vn} #{to_nid}")
        # change routing data at the vnode and synchronize a data
        nids << to_nid
        return false unless @routing_table.transaction(vn, nids)

        # synchronize a data
        @storages.each_key do |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)

          if res != 'STORED'
            @routing_table.rollback(vn)
            @logger.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return :abort if res.start_with?('SERVER_ERROR')
            return false
          end
        end

        if (clk = @routing_table.commit(vn)) == false
          @routing_table.rollback(vn)
          @logger.error("#{__method__}:routing table commit failed")
          return false
        end

        clk = @routing_table.set_route(vn, clk, new_nids)
        if clk.is_a?(Integer) == false
          clk, new_nids = @routing_table.search_nodes_with_clk(vn)
        end

        cmd = "setroute #{vn} #{clk - 1}"
        new_nids.each { |nn| cmd << " #{nn}" }
        res = async_broadcast_cmd("#{cmd}\r\n")
        @logger.debug("#{__method__}:async_broadcast_cmd(#{cmd}) #{res}")
      end

      return true
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
      false
    end

    def sync_a_vnode(vn, to_nid, is_primary = nil)
      nids = @routing_table.search_nodes(vn)

      if nids.include?(to_nid) == false || (is_primary && nids[0] != to_nid)
        @logger.debug("#{__method__}:#{vn} #{to_nid} #{is_primary}")
        # change routing data at the vnode and synchronize a data
        nids << to_nid
        return false unless @routing_table.transaction(vn, nids)

        # synchronize a data
        @storages.each_key do |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)

          if res != 'STORED'
            @routing_table.rollback(vn)
            @logger.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        end

        if (clk = @routing_table.commit(vn)) == false
          @routing_table.rollback(vn)
          @logger.error("#{__method__}:routing table commit failed")
          return false
        end

        nids = edit_nodes(nids, to_nid, is_primary)
        clk = @routing_table.set_route(vn, clk, nids)
        if clk.is_a?(Integer) == false
          clk, nids = @routing_table.search_nodes_with_clk(vn)
        end

        cmd = "setroute #{vn} #{clk - 1}"
        nids.each { |nn| cmd << " #{nn}" }
        res = async_broadcast_cmd("#{cmd}\r\n")
        @logger.debug("#{__method__}:async_broadcast_cmd(#{cmd}) #{res}")
      else
        # synchronize a data
        @storages.each_key do |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)
          if res != 'STORED'
            @logger.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        end
      end

      return true
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
      false
    end

    def edit_nodes(nodes, new_nid, is_primary)
      if @routing_table.rn == 1
        return [new_nid]
      end
      # [node_a, node_b, new_nid]
      nodes.delete(new_nid)
      # [node_a, node_b]

      if nodes.length >= @routing_table.rn
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
        nodes.insert(0, new_nid)
      else
        # [node_a, new_nid]
        nodes << new_nid
      end
      nodes
    end

    def push_a_vnode_stream(hname, vn, nid)
      @logger.debug("#{__method__}:hname=#{hname} vn=#{vn} nid=#{nid}")

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
          @logger.error("#{__method__}:canceled in hname=#{hname} vn=#{vn} nid=#{nid}")
          return 'CANCELED'
        end

        con.write(data)
        sleep @stats.stream_copy_wait_param
      end
      con.write("\0" * 20) # end of stream

      res = con.gets # STORED\r\n or error string
      Roma::Messaging::ConPool.instance.return_connection(nid, con)
      res.chomp! if res
      if res_dump == false
        @logger.error("#{__method__}:each_vn_dump in hname=#{hname} vn=#{vn} nid=#{nid}")
        return 'CANCELED'
      end
      res
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
      e.to_s
    end

    def asyncev_start_storage_clean_up_process(_args)
      #      @logger.info("#{__method__}")
      if @stats.run_storage_clean_up
        @logger.error("#{__method__}:already in being")
        return
      end
      @stats.run_storage_clean_up = true
      t = Thread.new do
        begin
          storage_clean_up_process
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
        ensure
          @stats.last_clean_up = Time.now
          @stats.run_storage_clean_up = false
        end
      end
      t[:name] = __method__
    end

    def storage_clean_up_process
      @logger.info("#{__method__}:start")
      me = @stats.ap_str
      vnhash = {}
      @routing_table.each_vnode do |vn, nids|
        if nids.include?(me)
          if nids[0] == me
            vnhash[vn] = :primary
          else
            vnhash[vn] = "secondary#{nids.index(me)}".to_sym
          end
        end
      end
      t = Time.now.to_i - Roma::Config::STORAGE_DELMARK_EXPTIME
      count = 0
      @storages.each_pair do |hname, st|
        break unless @stats.do_clean_up?
        st.each_clean_up(t, vnhash) do |key, vn|
          # @logger.debug("#{__method__}:key=#{key} vn=#{vn}")
          if @stats.run_receive_a_vnode.key?("#{hname}_#{vn}")
            false
          else
            nodes = @routing_table.search_nodes_for_write(vn)
            if nodes && nodes.length > 1
              nodes[1..-1].each do |nid|
                res = async_send_cmd(nid, "out #{key}\e#{hname} #{vn}\r\n")
                unless res
                  @logger.warn("send out command failed:#{key}\e#{hname} #{vn} -> #{nid}")
                end
                # @logger.debug("#{__method__}:res=#{res}")
              end
            end
            count += 1
            @stats.out_count += 1
            true
          end
        end
      end
      if count > 0
        @logger.info("#{__method__}:#{count} keys deleted.")
      end

      # delete @routing_table.logs
      if @stats.gui_run_gather_logs || @routing_table.logs.empty?
        false
      else
        gathered_time = @routing_table.logs[0]
        # delete gathering log data after 5min
        @routing_table.logs.clear if gathered_time.to_i < Time.now.to_i - (60 * 5)
      end
    ensure
      @logger.info("#{__method__}:stop")
    end

    def asyncev_calc_latency_average(args)
      latency, cmd = args
      # @logger.debug(__method__)

      unless @stats.latency_data.key?(cmd) # only first execute target cmd
        @stats.latency_data[cmd].store('latency', [])
        @stats.latency_data[cmd].store('latency_max', {})
        @stats.latency_data[cmd]['latency_max'].store('current', 0)
        @stats.latency_data[cmd].store('latency_min', {})
        @stats.latency_data[cmd]['latency_min'].store('current', 99_999)
        @stats.latency_data[cmd].store('time', Time.now.to_i)
      end

      begin
        @stats.latency_data[cmd]['latency'].delete_at(0) if @stats.latency_data[cmd]['latency'].length >= 10
        @stats.latency_data[cmd]['latency'].push(latency)

        @stats.latency_data[cmd]['latency_max']['current'] = latency if latency > @stats.latency_data[cmd]['latency_max']['current']
        @stats.latency_data[cmd]['latency_min']['current'] = latency if latency < @stats.latency_data[cmd]['latency_min']['current']

      rescue => e
        @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")

      ensure
        if @stats.latency_check_time_count && Time.now.to_i - @stats.latency_data[cmd]['time'] > @stats.latency_check_time_count
          average = @stats.latency_data[cmd]['latency'].inject(0.0) { |r, i| r += i } / @stats.latency_data[cmd]['latency'].size
          max = @stats.latency_data[cmd]['latency_max']['current']
          min = @stats.latency_data[cmd]['latency_min']['current']
          @logger.debug("Latency average[#{cmd}]: #{sprintf('%.8f', average)}"\
                     "(denominator=#{@stats.latency_data[cmd]['latency'].length}"\
                     " max=#{sprintf('%.8f', max)}"\
                     " min=#{sprintf('%.8f', min)})"
                    )

          @stats.latency_data[cmd]['time'] =  Time.now.to_i
          @stats.latency_data[cmd]['latency_past'] = @stats.latency_data[cmd]['latency']
          @stats.latency_data[cmd]['latency'] = []
          @stats.latency_data[cmd]['latency_max']['past'] = @stats.latency_data[cmd]['latency_max']['current']
          @stats.latency_data[cmd]['latency_max']['current'] = 0
          @stats.latency_data[cmd]['latency_min']['past'] = @stats.latency_data[cmd]['latency_min']['current']
          @stats.latency_data[cmd]['latency_min']['current'] = 99_999
        end
      end
      true
    end

    def asyncev_start_storage_flush_process(args)
      hname, dn = args
      @logger.debug("#{__method__} #{args.inspect}")

      st = @storages[hname]
      if st.dbs[dn] != :safecopy_flushing
        @logger.error("Can not flush storage. stat = #{st.dbs[dn]}")
        return true
      end
      t = Thread.new do
        begin
          st.flush_db(dn)
          st.set_db_stat(dn, :safecopy_flushed)
          @logger.info("#{__method__}:storage has flushed. (#{hname}, #{dn})")
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
        ensure
        end
      end
      t[:name] = __method__
      true
    end

    def asyncev_start_storage_cachecleaning_process(args)
      hname, dn = args
      @logger.debug("#{__method__} #{args.inspect}")

      st = @storages[hname]
      if st.dbs[dn] != :cachecleaning
        @logger.error("Can not start cachecleaning process. stat = #{st.dbs[dn]}")
        return true
      end
      t = Thread.new do
        begin
          storage_cachecleaning_process(hname, dn)
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
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
        break if keys.nil? || keys.length == 0
        break unless @do_storage_cachecleaning_process

        # @logger.debug("#{__method__}:#{keys.length} keys found")

        # copy cache -> db
        st.each_cache_by_keys(dn, keys) do |vn, last, clk, expt, k, v|
          break unless @do_storage_cachecleaning_process
          if st.load_stream_dump_for_cachecleaning(vn, last, clk, expt, k, v)
            count += 1
            # @logger.debug("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was stored.")
          else
            rcount += 1
            # @logger.debug("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was rejected.")
          end
        end

        # remove keys in a cache
        keys.each { |key| st.out_cache(dn, key) }
      end
      if @do_storage_cachecleaning_process == false
        @logger.warn("#{__method__}:uncompleted")
      else
        st.set_db_stat(dn, :normal)
      end
      @logger.debug("#{__method__}:#{count} keys loaded.")
      @logger.debug("#{__method__}:#{rcount} keys rejected.") if rcount > 0
    ensure
      @do_storage_cachecleaning_process = false
    end

    def asyncev_start_get_routing_event(args)
      @logger.debug("#{__method__} #{args}")
      t = Thread.new do
        begin
          get_routing_event
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
        ensure
        end
      end
      t[:name] = __method__
    end

    def get_routing_event
      @logger.info("#{__method__}:start.")

      routing_path = Config::RTTABLE_PATH
      f_list = Dir.glob("#{routing_path}/#{@stats.ap_str}*")

      f_list.each do|fname|
        IO.foreach(fname)do|line|
          if line =~ /join|leave/
            @routing_table.event.shift if @routing_table.event.size >= @routing_table.event_limit_line
            @routing_table.event << line.chomp
          end
        end
      end

      @logger.info("#{__method__} has done.")
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
    end

    def asyncev_start_get_logs(args)
      @logger.debug("#{__method__} #{args}")
      t = Thread.new do
        begin
          get_logs(args)
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
        ensure
          @stats.gui_run_gather_logs = false
        end
      end
      t[:name] = __method__
    end

    def get_logs(args)
      @logger.debug("#{__method__}:start.")

      log_path =  Config::LOG_PATH
      log_file = "#{log_path}/#{@stats.ap_str}.log"

      target_logs = []
      File.open(log_file)do|f|
        start_point = get_point(f, args[0], 'start')
        end_point = get_point(f, args[1], 'end')

        ## read target logs
        f.seek(start_point, IO::SEEK_SET)
        target_logs = f.read(end_point - start_point)
        target_logs = target_logs.each_line.map(&:chomp)
        target_logs.delete('.')
      end

      @routing_table.logs = target_logs
      # set gathered date for expiration
      @routing_table.logs.unshift(Time.now)

      @logger.debug("#{__method__} has done.")
    rescue => e
      @routing_table.logs = []
      @logger.error("#{e}\n#{$ERROR_POSITION}")
    ensure
      @stats.gui_run_gather_logs = false
    end

    def get_point(f, target_time, type, latency_time = Time.now, current_pos = 0, new_pos = f.size / 2)
      # hilatency check
      ps = Time.now - latency_time
      if ps > 5
        @logger.warn('gather_logs process was failed.')
        fail
      end

      # initialize read size
      read_size = 2048

      # first check
      unless target_time.class == Time
        # in case of not set end_date
        return f.size if target_time == 'current'

        target_time =~ (/(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)/)
        target_time = Time.mktime(Regexp.last_match[1], Regexp.last_match[2], Regexp.last_match[3], Regexp.last_match[4], Regexp.last_match[5], Regexp.last_match[6], 000000)

        # check outrange or not
        f.seek(0, IO::SEEK_SET)
        begining_log = f.read(read_size)
        pos = begining_log.index(/[IDEW],\s\[(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)\.(\d+)/)
        begining_time = Time.mktime(Regexp.last_match[1], Regexp.last_match[2], Regexp.last_match[3], Regexp.last_match[4], Regexp.last_match[5], Regexp.last_match[6], Regexp.last_match[7])

        f.seek(-read_size, IO::SEEK_END)
        end_log = f.read(read_size)
        pos = end_log.rindex(/[IDEW],\s\[(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)\.(\d+)/)
        end_time = Time.mktime(Regexp.last_match[1], Regexp.last_match[2], Regexp.last_match[3], Regexp.last_match[4], Regexp.last_match[5], Regexp.last_match[6], Regexp.last_match[7])

        case type
        when 'start'
          if target_time < begining_time
            return 0
          elsif target_time > end_time
            @logger.error('irregular time was set.')
            fail
          end
        when 'end'
          if target_time > end_time
            return f.size
          elsif target_time < begining_time
            @logger.error('irregular time was set.')
            fail
          end
        end
      end

      # read half sector size
      f.seek(new_pos, IO::SEEK_SET)
      sector_log = f.read(read_size)
      # grep date
      date_a = sector_log.scan(/[IDEW],\s\[(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)\.(\d+)/)

      time_a = []
      date_a.each do|time|
        time_a.push(Time.mktime(time[0], time[1], time[2], time[3], time[4], time[5], time[6]))
      end
      sector_time_first = time_a[0]
      sector_time_last = time_a[-1]

      if target_time.between?(sector_time_first, sector_time_last)
        time_a.each do|time|
          if target_time <= time
            time_string = time.strftime('%Y-%m-%dT%H:%M:%S')
            target_index = sector_log.index(/[IDEW],\s\[#{time_string}/)
            return new_pos + target_index
          end
        end
      elsif sector_time_first > target_time
        target_pos = new_pos - ((new_pos - current_pos).abs / 2)
      elsif sector_time_first < target_time
        target_pos = new_pos + ((new_pos - current_pos).abs / 2)
      end

      get_point(f, target_time, type, latency_time, new_pos, target_pos)
    end

    def asyncev_start_replicate_existing_data_process(args)
      # args is [$roma.cr_writer.replica_rttable])
      t = Thread.new do
        begin
          $roma.cr_writer.run_existing_data_replication = true
          replicate_existing_data_process(args)
        rescue => e
          @logger.error("#{__method__}:#{e.inspect} #{$ERROR_POSITION}")
        ensure
          $roma.cr_writer.run_existing_data_replication = false
        end
      end
      t[:name] = __method__
    end

    def replicate_existing_data_process(args)
      @logger.info("#{__method__} :start.")

      @storages.each_key do |hname|
        @routing_table.v_idx.each_key do |vn|
          raise unless $roma.cr_writer.run_existing_data_replication
          args[0].v_idx[vn].each do |replica_nid|
            res = push_a_vnode_stream(hname, vn, replica_nid)
            if res != 'STORED'
              @logger.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
              return false
            end
          end
        end
      end

      @logger.info("#{__method__} has done.")
    rescue => e
      @logger.error("#{e}\n#{$ERROR_POSITION}")
    end

  end # module AsyncProcess
end # module Roma
