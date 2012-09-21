module Roma
  module Routing
    module RandomPartitioner

      # vnode sampling without +without_nodes+
      def select_vn_for_join(without_nodes)
        short_idx = {}
        idx = {}
        @rd.v_idx.each_pair do |vn, nids|
          unless list_include?(nids, without_nodes)
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
    end
  end
end
