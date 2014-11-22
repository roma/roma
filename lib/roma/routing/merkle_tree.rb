require 'digest/sha1'

module Roma
  module Routing

    class MerkleTree
      
      attr :tree
      attr :div_bits
      attr :dgst_bits

      def initialize(dgst_bits,div_bits)
        @tree = {}
        @dgst_bits = dgst_bits
        @div_bits = div_bits
        create_tree_instance('0')
      end

      def set(vn,nodes)
        id = '0' + (vn >> (@dgst_bits-@div_bits)).to_s(2).rjust(@div_bits,'0')
        @tree[id] = Digest::SHA1.hexdigest(nodes.to_s)
        update(parent(id))
      end

      def get(id)
        @tree[id]
      end

      def to_vn(id)
        id[1,id.length].to_i(2) << (@dgst_bits-@div_bits)
      end

      private

      def create_tree_instance(id)
        @tree[id] = 0
        return if id.length > @div_bits
        create_tree_instance("#{id}0")
        create_tree_instance("#{id}1")
      end

      def update(id)
        @tree[id] = Digest::SHA1.hexdigest("#{@tree[id+'0']}:#{@tree[id+'1']}")
        update(parent(id)) if id.length != 1
      end

      def parent(id)
        id.chop
      end

    end # class MerkleTree

  end # module Routing
end # module Roma
