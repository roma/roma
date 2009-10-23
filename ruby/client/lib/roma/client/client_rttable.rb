require 'yaml'
require 'digest/sha1'

module Roma
  module Client

    class ClientRoutingTable

      attr :rd
      attr :search_mask
      attr :fail_cnt
      attr :hbits
      attr :rn
      attr :div_bits
      attr_accessor :mklhash

      def initialize(rd)
        @rd = rd
        @rn = @rd.rn
        @div_bits=@rd.div_bits
        @hbits = 2**@rd.dgst_bits
        @search_mask = @rd.search_mask
        @fail_cnt = Hash.new(0)
        @mklhash = nil
      end

      def nodes
        @rd.nodes.clone
      end

      def vnodes
        @rd.v_idx.keys
      end

      # Returns a vnode-id from digest.
      def get_vnode_id(d)
        d & @search_mask
      end

      # Returns a node-is list at the vnode. 
      # +vn+: vnode-id
      def search_nodes(vn)
        @rd.v_idx[vn].clone
      rescue
        nil
      end

      def search_node(key)
        d = Digest::SHA1.hexdigest(key).hex % @hbits
        nodes = @rd.v_idx[d & @search_mask]
        nodes.each_index { |i|
          return [nodes[i], d] if @fail_cnt[nodes[i]] == 0
        }
        # for expecting an auto assign process
        svn = vn = d & @search_mask
        while( (vn = @rd.next_vnode(vn)) != svn )
          nodes = @rd.v_idx[vn]
          nodes.each_index { |i|
            return [nodes[i], d] if @fail_cnt[nodes[i]] == 0
          }
        end
        nil
      rescue => e
        p e
        nil
      end

      def proc_failed(nid)
        @fail_cnt[nid] += 1
        @mklhash = 0
      end

    end # class ClientRoutingTable

  end # module Client
end # module Roma
