require 'roma/routing/rttable'

module Roma
  module Routing

    class ChurnbasedRoutingTable < RoutingTable

      attr :fname
      attr :log_fd
      attr :log_name
      attr :trans # transaction
      attr :leave_proc
      attr :lost_proc
      attr_accessor :lost_action
      attr_reader :version_of_nodes
      attr_reader :min_version

      def initialize(rd,fname)
        super(rd)
        @rd.nodes.sort!
        @trans={}
        @fname=fname
        @leave_proc=nil
        @lost_proc=nil
        @lost_action=:no_action
        @enabled_failover=false
        @lock = Mutex.new
        @version_of_nodes = Hash.new(0)
        @min_version = nil
        open_log
      end

      def get_stat(ap)
        ret = super(ap)
        ret['routing.lost_action'] = @lost_action.to_s
        ret['routing.version_of_nodes'] = @version_of_nodes.inspect
        ret['routing.min_version'] = @min_version
        ret
      end
      
      def set_version(nid,ver)
        @version_of_nodes[nid] = ver
        if @min_version == nil || @min_version > ver
          @min_version = ver
        end
      end

      def find_min_version
        ret = 0xffffff
        @version_of_nodes.each_value{|ver| ret = ver if ret > ver}
        ret
      end

      def set_leave_proc(&block)
        @leave_proc=block        
      end

      def set_lost_proc(&block)
        @lost_proc=block        
      end

      def open_log
        log_list=@rd.get_file_list(@fname)
        if log_list.length==0
          @log_name="#{@fname}.1"
        else
          if File::stat("#{@fname}.#{log_list.last[0]}").size == 0
            @log_name="#{@fname}.#{log_list.last[0]}"
          else
            @log_name="#{@fname}.#{log_list.last[0]+1}"
          end
        end
        @log_fd=File.open(@log_name,"a")      
      end

      def write_log_setroute(vn, clk, nids)
        log="setroute #{vn} #{clk}"
        nids.each{ |nid| log << " #{nid}" }
        write_log(log)        
      end

      def write_log(line)
        # log rotation
        if File::stat(@log_name).size > 1000 * 1024
          close_log
          open_log
        end
        t = Time.now
        tstr = "#{t.strftime('%Y-%m-%dT%H:%M:%S')}.#{t.usec}"
        @log_fd.write("#{tstr} #{line}\n")
        @log_fd.flush
      end

      def close_log
        @log_fd.close
      end

      def can_i_recover?
        @rd.nodes.length >= @rd.rn
      end

      # Retuens the list of losted-data vnode newer than argument time.
      def search_lost_vnodes(t)
        ret = []
        @rd.each_log_all(@fname){|log_t,line|
          next if t > log_t
          s = line.split(/ /)
          if s[0] == 'setroute' && s.length == 3
            # vnode has a no pnode. therefor this vnode was lost.
            ret << s[1].to_i
          end
        }
        ret
      end

      # select a vnodes where short of redundancy.
      def select_a_short_vnodes(exclued_nodes)
        ret = []
        @rd.v_idx.each_pair{|vn, nids|
          if nids.length < @rd.rn && list_include?(nids,exclued_nodes) == false
            ret << [vn,nids]
          end
        }
        ret
      end

      # vnode sampling without +without_nodes+
      def sample_vnode(without_nodes)
        short_idx = {}
        idx = {}
        @rd.v_idx.each_pair{|vn, nids|
          unless list_include?(nids, without_nodes)
            idx[vn] = nids
            short_idx[vn] = nids if nids.length < @rd.rn
          end
        }
        idx = short_idx if short_idx.length > 0
          
        ks = idx.keys
        return nil if ks.length == 0
        vn = ks[rand(ks.length)]
        nids = idx[vn]
        [vn, nids]
      end

      def list_include?(list,nodes)
        nodes.each{|nid|
          return true if list.include?(nid)
        }
        false
      end
      private :list_include?

      def set_route(vn, clk, nids)
        return "#{vn} is not found." unless @rd.v_idx.key?(vn)
        @lock.synchronize {
          return "It's old table." if @rd.v_clk[vn] > clk
          nids.each{ |nid|
            add_node(nid) unless @rd.nodes.include?(nid)
          }
          @rd.v_idx[vn] = nids.clone
          clk += 1
          @rd.v_clk[vn] = clk
          @mtree.set(vn, nids)
          write_log_setroute(vn, clk, nids)
          return clk
        }
      end

      def add_node(nid)
        unless @rd.nodes.include?(nid)
          @rd.nodes << nid
          @rd.nodes.sort!
          write_log("join #{nid}")
        end
      end

      def enabled_failover=(b)
        @enabled_failover=b
        @fail_cnt.clear
      end

      def enabled_failover
        @enabled_failover
      end

      def leave(nid)
        unless @enabled_failover
          return
        end
        return unless @rd.nodes.include?(nid)

        @leave_proc.call(nid) if @leave_proc
        @rd.nodes.delete(nid)
        @version_of_nodes.delete(nid)
        @min_version = find_min_version
        
        @log.warn("#{nid} just failed.")
        write_log("leave #{nid}")
        
        lost_vnodes=[]
        @lock.synchronize {
          @rd.v_idx.each_pair{ |vn, nids|
            buf = nids.clone
            if buf.delete(nid)
              set_route_and_inc_clk_inside_sync(vn, buf)
              if buf.length == 0
                lost_vnodes << vn
                @log.error("Vnode data is lost.(Vnode=#{vn})")
               end
            end
          }
        }
        if lost_vnodes.length > 0
          @lost_proc.call if @lost_proc
          if @lost_action == :auto_assign
            lost_vnodes.each{ |vn|
              set_route_and_inc_clk_inside_sync( vn, next_alive_vnode(vn) )
            }
          end
        end
        @fail_cnt.delete(nid)
      end

      def set_route_and_inc_clk_inside_sync(vn, nodes)
        @rd.v_idx[vn] = nodes
        clk = @rd.v_clk[vn] + 1
        @rd.v_clk[vn] = clk
        @mtree.set(vn, nodes)
        write_log_setroute(vn, clk, nodes)
        clk
      end
      private :set_route_and_inc_clk_inside_sync

      def next_alive_vnode(vn)
        svn = vn
        while( (vn = @rd.next_vnode(vn)) != svn )
          return @rd.v_idx[vn].clone if @rd.v_idx[vn].length != 0
        end
        []
      end
      private :next_alive_vnode

      def each_vnode
        @rd.v_idx.each_pair{ |k, v| yield(k, v) }
      end

      def v_idx
        @rd.v_idx.clone
      end

      def search_nodes_for_write(vn)
        return @trans[vn][0].clone if @trans.key?(vn)
        @rd.v_idx[vn].clone
      rescue
        nil
      end

      def search_nodes_with_clk(vn)
        @lock.synchronize {
          return [@rd.v_clk[vn], @rd.v_idx[vn].clone]
        }
      rescue
        nil
      end

      # +vn+: vnode-id
      # +nids+: node-id list
      def transaction(vn, nids)
        return false if @trans.key?(vn)
        @trans[vn]=[nids.clone, Time.now]
        true
      end

      def commit(vn)
        return false unless @trans.key?(vn)
        @lock.synchronize {
          @rd.v_idx[vn]=@trans[vn][0]
          @trans.delete(vn)
          clk = @rd.v_clk[vn] + 1
          @rd.v_clk[vn] = clk
          @mtree.set(vn, @rd.v_idx[vn])
          write_log_setroute(vn, clk, @rd.v_idx[vn])
          return clk
        }
      end

      def rollback(vn)
        @trans.delete(vn)
      end

      # +sec+: elapsed-time
      def delete_old_trans(sec=3600)
        @trans.delete_if{|vn,val| val[1] < Time.now-sec }
      end

      # Returns the status of vnode balance.
      # +ap+: my address_port string(ex."roma0_11211")
      def vnode_balance(ap)
        # amount of primary at node = amount of vnode / amount of node
        n = (2**div_bits) / nodes.length

        pcount = scount = 0
        @rd.v_idx.each_pair{ |vn, nids|
          next if nids == nil or nids.length == 0
          if nids[0] == ap
            pcount += 1
          elsif nids.include?(ap)
            scount += 1
          end
        }

        @log.debug("#{__FILE__}:#{__LINE__}:n=#{n} pcount=#{pcount} scount=#{scount}")

        if pcount > n*1.1
          return :over
        elsif pcount < n*0.9
          return :less
        end
        :even
      end

    end # class ChurnbasedRoutingTable

  end # module Routing
end # module Roma
