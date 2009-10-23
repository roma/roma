
module Roma
  module Storage

    class DummyStorage

      def initialize; end
      def init(*args); end
      def opendb(fname,divnum); end
      def closedb; end
      def get_context(vn, k, d) nil; end
      def rset(vn, k, d, lclock, exptime, v); end
      def set(vn, k, d, exptime, v); end
      def add(vn, k, d, exptime, v); end
      def replace(vn, k, d, exptime, v); end
      def append(vn, k, d, exptime, v); end
      def prepend(vn, k, d, exptime, v); end
      def get(vn,k,d); 'dummy'; end
      def rdelete(vn,k,d,lclock); end
      def delete(vn,k,d); end
      def incr(vn, k, d, v); end
      def decr(vn, k, d, v); end
      def true_length; 100; end
      def add_vnode(vn); end
      def del_vnode(vn); end
      def clean_up(t); end

      def load(dmp); 10 end

      # Returns the vnode dump.
      def dump(vn)
        Marshal.dump(get_vnode_hash(vn))
      end

      private

      # Create vnode dump.
      def get_vnode_hash(vn)
        {'dummy'=>'dummy'}
      end
    end

  end
end
