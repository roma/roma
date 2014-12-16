require 'roma/storage/basic_storage'

module Roma
  module Storage

    class Dummy
      def put(k,v)
        raise StorageException, "error:get"
      end
      def get(k)
        raise StorageException, "error:get"
      end
      def out(k)
        true
      end
      def rnum
        0
      end
      def each
      end
    end

    class StorageErrorStorage < BasicStorage

      def opendb
        create_div_hash
        @divnum.times{ |i|
          @hdb[i] = Dummy.new
        }
      end

      def close_db(hdb); end

    end # class StorageErrorStorage

  end # module Storage
end # module Roma
