require 'gdbm'
require 'roma/storage/basic_storage'

module Roma
  module Storage

    module GDBM_Ext
      def put(k,v); self[k] = v; end
      def get(k); self[k]; end
      def out(k); delete(k); end
      def rnum; length; end
    end

    class DbmStorage < BasicStorage

      def initialize
        super
        @ext_name = 'dbm'
      end

      def clean_up(t,unit_test_flg=nil)
        n = 0
        nt = Time.now.to_i
        @hdb.each_index{ |i|
          delkey = []
          @hdb[i].each{ |k, v|
            vn, last, clk, expt = unpack_header(v)
            if nt > expt && t > last
              n += 1
              delkey << k
            end
            if unit_test_flg
              closedb
            end
          }
          delkey.each{ |k| @hdb[i].out(k) }
        }
        n
      rescue => e
        raise NoMethodError(e.message)
      end

   def each_clean_up(t, vnhash)
        @do_clean_up = true
        nt = Time.now.to_i
        @hdb.each{ |hdb|
          delkey = []
          hdb.each{ |k, v|
            return unless @do_clean_up
            vn, last, clk, expt = unpack_header(v)
            vn_stat = vnhash[vn]
            if vn_stat == :primary && ( (expt != 0 && nt > expt) || (expt == 0 && t > last) )
              delkey << [vn, k, v]
            elsif vn_stat == nil && t > last
              delkey << [vn, k, v]
            end
            sleep @each_clean_up_sleep
          }
          delkey.each{ |vn, k, v|
            yield k, vn
            hdb.out(k) if hdb.get(k) == v
          }
        }
      end

      private

      def open_db(fname)
        hdb = GDBM::open(fname,0666)
        raise RuntimeError.new("dbm open error.") unless hdb
        hdb.extend(Roma::Storage::GDBM_Ext)
        return hdb
      end

      def close_db(hdb); hdb.close; end

    end # class DbmStorage

  end # module Storage
end # module Roma
