require 'roma/storage/basic_storage'

module Roma
  module Storage

    module RH_Ext
      def put(k,v); self[k] = v; end
      def get(k); self[k]; end
      def out(k); delete(k); end
      def rnum; length; end
      def sync; true; end
    end

    class RubyHashStorage < BasicStorage

      def opendb
        create_div_hash
        @divnum.times do |i|
          @hdb[i] = open_db(nil)
          @hdbc[i] = nil
          @dbs[i] = :normal
        end
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

        def each_unpacked_db(target_vn, db)
          count = 0
          tn =  Time.now.to_i
          keys = db[@hdiv[target_vn]].keys
          keys.each do |k|
            v = db[@hdiv[target_vn]][k]
            vn, last, clk, expt, val = unpack_data(v)
            if vn != target_vn || (expt != 0 && tn > expt)
              count += 1
              sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
              next
            end
            yield vn, last, clk, expt, k, val
          end
        end
        private :each_unpacked_db


        def each_hdb_dump(i,except_vnh = nil)
          count = 0
          keys = @hdb[i].keys
          keys.each{|k|
            v = @hdb[i][k]
            vn, last, clk, expt, val = unpack_data(v)
            if except_vnh && except_vnh.key?(vn) || Time.now.to_i > expt
              count += 1
              sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
            else
              yield [vn, last, clk, expt, k.length, k, val.length, val].pack("NNNNNa#{k.length}Na#{val.length}")
              sleep @each_vn_dump_sleep
            end
          }
        end

        # Create vnode dump.
        def get_vnode_hash(vn)
          buf = {}
          count = 0
          hdb = @hdb[@hdiv[vn]]
          keys = hdb.keys
          keys.each{ |k|
            v = hdb[k]
            count += 1
            sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
            dat = unpack_data(v) #v.unpack('NNNN')
            buf[k] = v if dat[0] == vn
          }
          return buf
        end
        private :get_vnode_hash
        
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
