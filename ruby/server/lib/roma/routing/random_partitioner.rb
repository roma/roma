module Roma
  module Routing
    module RandomPartitioner

      def exclude_nodes(ap_str, rep_host)
        exclude_nodes = nodes
        if rep_host
          exclude_nodes = [ap_str]
        else
          myhost = ap_str.split(/[:_]/)[0]
          exclude_nodes.delete_if{|nid| nid.split(/[:_]/)[0] != myhost }
        end
        exclude_nodes
      end
      
      alias :exclude_nodes_for_join :exclude_nodes
      alias :exclude_nodes_for_recover :exclude_nodes

      # vnode sampling exclude +exclude_nodes+
      def select_vn_for_join(exclude_nodes)
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
        [vn, nids]
      end

      # select a vnodes where short of redundancy.
      def select_vn_for_recover(exclued_nodes)
        ret = []
        @rd.v_idx.each_pair do |vn, nids|
          if nids.length < @rd.rn && list_include?(nids,exclued_nodes) == false
            ret << [vn,nids]
          end
        end
        if ret.length == 0
          nil
        else
          n = rand(ret.length)
          [ret[n][0], ret[n][1], (rand(@rd.rn) == 0)]
        end
      end

      def select_vn_for_release(exclued_nodes)
        # TODO
      end

    end
  end
end
