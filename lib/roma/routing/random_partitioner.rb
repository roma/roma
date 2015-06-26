module Roma
  module Routing
    module RandomPartitioner
      def exclude_nodes(ap_str, rep_host)
        exclude_nodes = nodes
        if rep_host
          exclude_nodes = [ap_str]
        else
          myhost = ap_str.split(/[:_]/)[0]
          exclude_nodes.delete_if { |nid| nid.split(/[:_]/)[0] != myhost }
        end
        exclude_nodes
      end

      def exclude_nodes_for_join(ap_str, _rep_host)
        [ap_str]
      end

      alias_method :exclude_nodes_for_recover, :exclude_nodes
      alias_method :exclude_nodes_for_balance, :exclude_nodes

      def myhost_include?(nodes, myhost)
        nodes.each do |nid|
          return true if nid.split(/[:_]/)[0] == myhost
        end
        false
      end
      private :myhost_include?

      # vnode sampling exclude +exclude_nodes+
      def select_vn_for_join(exclude_nodes, rep_host)
        short_idx = {}
        myhost_idx = {}
        idx = {}
        myhost = exclude_nodes[0].split(/[:_]/)[0]

        @rd.v_idx.each_pair do |vn, nids|
          unless list_include?(nids, exclude_nodes)
            if myhost_include?(nids, myhost)
              myhost_idx[vn] = nids
            else
              idx[vn] = nids # other hosts
            end
            short_idx[vn] = nids if nids.length < @rd.rn
          end
        end

        # vnodes sampling priority:
        # 1. Short vnodes
        # 2. Other hosts vnodes
        # 3. My host vnodes
        if short_idx.length > 0
          idx = short_idx
        elsif idx.length == 0
          idx = myhost_idx
        end

        return nil if idx.length == 0

        ks = idx.keys
        vn = ks[rand(ks.length)]
        nids = idx[vn]

        if !rep_host && nids[0].split(/[:_]/)[0] == myhost
          is_primary = true
        else
          is_primary = rand(@rd.rn) == 0
        end

        [vn, nids, is_primary]
      end

      # select a vnodes where short of redundancy.
      def select_vn_for_recover(exclude_nodes, _rep_host)
        ret = []
        @rd.v_idx.each_pair do |vn, nids|
          if nids.length < @rd.rn && list_include?(nids, exclude_nodes) == false
            ret << [vn, nids]
          end
        end
        if ret.length == 0
          nil
        else
          n = rand(ret.length)
          [ret[n][0], ret[n][1], (rand(@rd.rn) == 0)]
        end
      end

      def select_node_for_release(ap_str, rep_host, nids)
        buf = nodes

        unless rep_host
          deny_hosts = []
          nids.each do |nid|
            host = nid.split(/[:_]/)[0]
            deny_hosts << host if host != ap_str.split(/[:_]/)[0]
          end
          buf.delete_if { |nid| deny_hosts.include?(nid.split(/[:_]/)[0]) }
        else
          nids.each { |nid| buf.delete(nid) }
        end

        buf.delete_if { |instance| instance == ap_str }
        to_nid = buf.sample
        new_nids = nids.map { |n| n == ap_str ? to_nid : n }
        [to_nid, new_nids]
      end

      # vnode sampling exclude +exclude_nodes+
      def select_vn_for_balance(exclude_nodes, _rep_host)
        short_idx = {}
        idx = {}
        @rd.v_idx.each_pair do |vn, nids|
          unless list_include?(nids, exclude_nodes)
            idx[vn] = nids
            short_idx[vn] = nids if nids.length < @rd.rn
          end
        end
        idx = short_idx if short_idx.length > 0

        ks = idx.keys
        return nil if ks.length == 0
        vn = ks[rand(ks.length)]
        nids = idx[vn]
        [vn, nids, rand(@rd.rn) == 0]
      end
    end
  end
end
