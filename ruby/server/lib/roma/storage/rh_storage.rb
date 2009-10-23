require 'roma/storage/basic_storage'

module Roma
  module Storage

    module RH_Ext
      def put(k,v); self[k] = v; end
      def get(k); self[k]; end
      def out(k); delete(k); end
      def rnum; length; end
    end

    class RubyHashStorage < BasicStorage

      def opendb
        create_div_hash
        @divnum.times{ |i|
          @hdb[i] = open_db(nil)
        }
      end

      private

      def open_db(fname)
        hdb = {}
        hdb.extend(Roma::Storage::RH_Ext)
        return hdb
      end

      def close_db(hdb); end

    end # class RubyHashStorage

  end # module Storage
end # module Roma
