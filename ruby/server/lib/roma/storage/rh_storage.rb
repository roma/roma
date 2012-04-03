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

      if RUBY_VERSION >= '1.9.2'
        def each_clean_up(t, vnhash)
          @do_clean_up = true
          nt = Time.now.to_i
          @hdb.each{ |hdb|
            keys = hdb.keys
            keys.each{ |k|
              v = hdb[k]
              return unless @do_clean_up
              vn, last, clk, expt = unpack_header(v)
              vn_stat = vnhash[vn]
              if vn_stat == :primary && ( (expt != 0 && nt > expt) || (expt == 0 && t > last) )
                yield k, vn
                hdb.out(k) if hdb.get(k) == v
              elsif vn_stat == nil && t > last
                yield k, vn
                hdb.out(k) if hdb.get(k) == v
              end
              sleep @each_clean_up_sleep
            }
          }
        end
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
