require 'digest/sha1'
require 'thread'

module Roma
  module Storage

    class StorageException < Exception; end

    class BasicStorage

      attr_reader :hdb
      attr_reader :hdiv
      attr_reader :ext_name
      attr_reader :error_message
      attr_reader :dbs

      attr_writer :vn_list
      attr_writer :storage_path
      attr_writer :option

      attr_accessor :divnum
      attr_accessor :each_vn_dump_sleep
      attr_accessor :each_vn_dump_sleep_count
      attr_accessor :each_clean_up_sleep
      attr_accessor :logic_clock_expire

      attr_accessor :do_each_vn_dump

      def initialize
        # database handler
        @hdb = []
        # database cache handler
        @hdbc = []
        # status of a database
        @dbs = []

        @hdiv = Hash.new(0)

        @ext_name = 'db'

        @divnum = 10

        @each_vn_dump_sleep = 0.001
        @each_vn_dump_sleep_count = 100
        @each_clean_up_sleep = 0.01
        @logic_clock_expire = 300

        @each_cache_lock = Mutex::new
        @each_clean_up_lock = Mutex::new
        @stat_lock = Mutex::new
      end

      def get_stat
        ret = {}
        ret['storage.storage_path'] = File.expand_path(@storage_path)
        ret['storage.divnum'] = @divnum
        ret['storage.option'] = @option
        ret['storage.each_vn_dump_sleep'] = @each_vn_dump_sleep
        ret['storage.each_vn_dump_sleep_count'] = @each_vn_dump_sleep_count
        ret['storage.each_clean_up_sleep'] = @each_clean_up_sleep
        ret['storage.logic_clock_expire'] = @logic_clock_expire
        ret['storage.safecopy_stats'] = @dbs.inspect
        ret
      end

      # Compare this clock with the specified.  
      #
      # -1, 0 or 1 as +clk1+ is numerically less than, equal to,
      # or greater than the +clk2+ given as the parameter.
      # 
      # logical clock space is a 32bit ring.
      def cmp_clk(clk1, clk2)
        if (clk1-clk2).abs < 0x80000000 # 1<<31
          clk1 <=> clk2
        else
          clk2 <=> clk1
        end
      end
      private :cmp_clk

      def create_div_hash
        @vn_list.each{ |vn|
          @hdiv[vn] = Digest::SHA1.hexdigest(vn.to_s).hex % @divnum
        }
      end
      protected :create_div_hash

      def mkdir_p(md_path)
        path = ''
        md_path.split('/').each do |p|
          if p.length == 0
            path = '/'
            next
          end
          path << p
          Dir::mkdir(path) unless File.exist?(path)
          path << '/'
        end
      end
      protected :mkdir_p

      def opendb
        create_div_hash
        mkdir_p(@storage_path)
        @divnum.times do |i|
          # open a database
          @hdb[i] = open_db("#{@storage_path}/#{i}.#{@ext_name}")
          # TODO
          # 1.open a status file
          # 2.open a cache file, if status is not in :normal status.
          @dbs[i] = :normal
          # opan a database cache
          @hdbc[i] = nil
        end
      end

      def closedb
        stop_clean_up
        buf = @hdb; @hdb = []
        buf.each{ |h| close_db(h) if h }
        buf = @hdbc; @hdbc = []
        buf.each{ |h| close_db(h) if h }
      end


      # [ 0.. 3] vn
      # [ 4.. 7] physical clock (unix time)
      # [ 8..11] logical clock
      # [12..15] exptime(unix time)
      # [16..  ] value data

      PACK_HEADER_TEMPLATE='NNNN'
      PACK_TEMPLATE=PACK_HEADER_TEMPLATE+'a*'
      def pack_header(vn, physical_clock, logical_clock, expire)
        [vn,physical_clock, logical_clock, expire].pack(PACK_HEADER_TEMPLATE)
      end
      def unpack_header(str)
        str.unpack(PACK_HEADER_TEMPLATE)
      end
      def pack_data(vn, physical_clock, logical_clock, expire,value)
        [vn,physical_clock, logical_clock, expire, value].pack(PACK_TEMPLATE)
      end
      def unpack_data(str)
        str.unpack(PACK_TEMPLATE)
      end
      private :pack_header, :unpack_header, :pack_data, :unpack_data

      def db_get(vn, k)
        n = @hdiv[vn]
        d = @hdb[n].get(k)
        return d if @dbs[n] == :normal

        c = @hdbc[n].get(k)
        return d unless c # in case of out of :normal status

        if @dbs[n] == :cachecleaning && d
          # in case of existing value is both @hdb and @hdbc
          vn, lat, clk, expt = unpack_header(d)
          cvn, clat, cclk, cexpt = unpack_header(c)
          return d if cmp_clk(clk, cclk) > 0 # if @hdb newer than @hdbc
        end
        c
      end

      def db_put(vn, k, v)
        n = @hdiv[vn]
        if @dbs[n] == :safecopy_flushing || @dbs[n] == :safecopy_flushed
          ret = @hdbc[n].put(k, v)
        else
          ret = @hdb[n].put(k, v)
        end
        ret
      end

      def get_context(vn, k, d)
        buf = db_get(vn, k)
        return nil unless buf
        unpack_header(buf)
      end

      def cas(vn, k, d, clk, expt, v)
        buf = db_get(vn ,k)
        return :not_found unless buf
        t = Time.now.to_i
        data = unpack_data(buf)
        return :not_found if t > data[3]
        return :exists if clk != data[2]
        clk = (data[2] + 1) & 0xffffffff
        ret = [vn, t, clk, expt, v]
        return ret if db_put(vn, k, pack_data(*ret))
        nil
      end

      def rset(vn, k, d, clk, expt, v)
        buf = db_get(vn, k)
        t = Time.now.to_i
        if buf
          data = unpack_data(buf)
          if t - data[1] < @logic_clock_expire && cmp_clk(clk,data[2]) <= 0
            @error_message = "error:#{t-data[1]} < #{@logic_clock_expire} && cmp_clk(#{clk},#{data[2]})<=0"
            return nil
          end
        end

        ret = [vn, t, clk, expt, v]
        return ret if db_put(vn, k, pack_data(*ret))
        @error_message = "error:put"
        nil
      end

      def set(vn, k, d, expt, v)
        buf = db_get(vn, k)
        clk = 0
        if buf
          data = unpack_data(buf)
          clk = (data[2] + 1) & 0xffffffff
        end

        ret = [vn, Time.now.to_i, clk, expt, v]
        return ret if db_put(vn , k, pack_data(*ret))
        nil
      end

      def add(vn, k, d, expt, v)
        buf = db_get(vn, k)
        clk = 0
        if buf
          vn, t, clk, expt2, v2 = unpack_data(buf)
          return nil if Time.now.to_i <= expt2
          clk = (clk + 1) & 0xffffffff
        end
        
        # not exist
        ret = [vn, Time.now.to_i, clk, expt, v]
        return ret if db_put(vn, k, pack_data(*ret))
        nil
      end

      def replace(vn, k, d, expt, v)
        buf = db_get(vn, k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        ret = [vn, Time.now.to_i, clk, expt, v]
        return ret if db_put(vn, k, pack_data(*ret))
        nil
      end

      def append(vn, k, d, expt, v)
        buf = db_get(vn, k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        ret = [vn, Time.now.to_i, clk, expt, v2 + v]
        return ret if db_put(vn, k, pack_data(*ret))
        nil
      end

      def prepend(vn, k, d, expt, v)
        buf = db_get(vn, k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        ret = [vn, Time.now.to_i, clk, expt, v + v2]
        return ret if db_put(vn, k, pack_data(*ret))
        nil
      end

      def get(vn, k, d)
        buf = db_get(vn, k)
        return nil unless buf
        vn, t, clk, expt, v = unpack_data(buf)

        return nil if Time.now.to_i > expt
        v
      end

      def get_raw(vn, k, d)
        buf = db_get(vn, k)
        return nil unless buf

        unpack_data(buf)
      end

      def get_raw2(k)
        @hdb.each{|hdb|
          buf = hdb.get(k)
          return unpack_data(buf) if buf
        }
        nil
      end

      def rdelete(vn, k, d, clk)
        buf = db_get(vn, k)
        t = Time.now.to_i
        if buf
          data = unpack_header(buf)
          if t - data[1] < @logic_clock_expire && cmp_clk(clk,data[2]) <= 0
            @error_message = "error:#{t-data[1]} < #{@logic_clock_expire} && cmp_clk(#{clk},#{data[2]})<=0"
            return nil 
          end
        end

        # [ 0.. 3] vn
        # [ 4.. 7] physical clock(unix time)
        # [ 8..11] logical clock
        # [12..15] exptime(unix time) => 0
        ret = [vn, t, clk, 0]
        if db_put(vn, k, pack_header(*ret))
          return ret
        else
          return nil
        end
      end

      def delete(vn, k, d)
        buf = db_get(vn, k)
        v = ret = nil
        clk = 0
        if buf
          vn, t, clk, expt, v2 = unpack_data(buf)
          return :deletemark if expt == 0
          clk = (clk + 1) & 0xffffffff
          v = v2 if v2 && v2.length != 0 && Time.now.to_i <= expt  
        end
 
        # [ 0.. 3] vn
        # [ 4.. 7] physical clock(unix time)
        # [ 8..11] logical clock
        # [12..15] exptime(unix time) => 0
        ret = [vn, Time.now.to_i, clk, 0, v]
        if db_put(vn, k, pack_header(*ret[0..-2]))
          return ret
        else
          return nil
        end
      end

      def out(vn, k, d)
        @hdb[@hdiv[vn]].out(k)
      end 

      def incr(vn, k, d, v)
        buf = db_get(vn, k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        v = (v2.to_i + v)
        v = 0 if v < 0
        v = v & 0xffffffffffffffff

        ret = [vn, Time.now.to_i, clk, expt2, v.to_s]
        return ret if db_put(vn, k, pack_data(*ret))
        nil
      end

      def decr(vn, k, d, v)
        buf = db_get(vn, k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        v = (v2.to_i - v)
        v = 0 if v < 0
        v = v & 0xffffffffffffffff

        ret = [vn, Time.now.to_i, clk, expt2, v.to_s]
        return ret if db_put(vn, k, pack_data(*ret))
        nil
      end

      # set expire time
      def set_expt(vn, k, d, expt)
        buf = db_get(vn, k)
        if buf
          vn, t, clk, expt2, v = unpack_data(buf)
          return nil if Time.now.to_i > expt2
          clk = (clk + 1) & 0xffffffff
          ret = [vn, Time.now.to_i, clk, expt, v]
          return ret if db_put(vn, k, pack_data(*ret))
        end
        nil
      end

      def true_length
        res = 0
        @hdb.each{ |hdb| res += hdb.rnum }
        res
      end

      def each_clean_up(t, vnhash)
        @do_clean_up = true
        return unless @each_clean_up_lock.try_lock
        nt = Time.now.to_i
        @divnum.times do |i|
          next if @dbs[i] != :normal
          hdb = @hdb[i]
          hdb.each do |k, v|
            return unless @do_clean_up # 1st check
            vn, last, clk, expt = unpack_header(v)
            vn_stat = vnhash[vn]
            if vn_stat == :primary && ( (expt != 0 && nt > expt) || (expt == 0 && t > last) )
              if yield k, vn
                hdb.out(k) if hdb.get(k) == v
              end
            elsif vn_stat == nil && t > last
              if yield k, vn
                hdb.out(k) if hdb.get(k) == v
              end
            end
            return unless @do_clean_up # 2nd ckeck 
            sleep @each_clean_up_sleep
          end
        end
      ensure
        @each_clean_up_lock.unlock if @each_clean_up_lock.locked?  
      end

      def stop_clean_up(&block)
        @do_clean_up = false
        if block
          @each_clean_up_lock.lock
          begin
            block.call
          ensure
            @each_clean_up_lock.unlock
          end
        end
      end

      def load(dmp)
        n = 0
        h = Marshal.load(dmp)
        h.each_pair{ |k, v|
          # remort data
          r_vn, r_last, r_clk, r_expt = unpack_header(v)
          raise "An invalid vnode number is include.key=#{k} vn=#{r_vn}" unless @hdiv.key?(r_vn)
          local = @hdb[@hdiv[r_vn]].get(k)
          if local == nil
            n += 1
            @hdb[@hdiv[r_vn]].put(k, v)
          else
            # local data
            l_vn, l_last, l_clk, l_expt = unpack_data(local)
            if r_last - l_last < @logic_clock_expire && cmp_clk(r_clk,l_clk) <= 0
            else # remort is newer.
              n += 1
              @hdb[@hdiv[r_vn]].put(k, v)
            end
          end
          sleep @each_vn_dump_sleep
        }
        n
      end

      def load_stream_dump(vn, last, clk, expt, k, v)
        buf = db_get(vn, k)
        if buf
          data = unpack_header(buf)
          if last - data[1] < @logic_clock_expire && cmp_clk(clk,data[2]) <= 0
            return nil
          end
        end

        ret = [vn, last, clk, expt, v]
        if expt == 0
          # for the deleted mark
          return ret if db_put(vn, k, pack_header(*ret[0..3]))
        else
          return ret if db_put(vn, k, pack_data(*ret))
        end
        nil
      end

      def load_stream_dump_for_cachecleaning(vn, last, clk, expt, k, v)
        n = @hdiv[vn]
        buf = @hdb[n].get(k)
        if buf
          data = unpack_header(buf)
          if last - data[1] < @logic_clock_expire && cmp_clk(clk,data[2]) <= 0
            return nil
          end
        end

        ret = [vn, last, clk, expt, v]
        if expt == 0
          # for the deleted mark
          return ret if @hdb[n].put(k, pack_header(*ret[0..3]))
        else
          return ret if @hdb[n].put(k, pack_data(*ret))
        end
        nil
      end

      # Returns the vnode dump.
      def dump(vn)
        buf = get_vnode_hash(vn)
        return nil if buf.length == 0
        Marshal.dump(buf)
      end

      def each_vn_dump(target_vn)
        @do_each_vn_dump = true
        n = @hdiv[target_vn]
        if @dbs[n] == :normal
          # in case of :normal status
          each_unpacked_db(target_vn, @hdb) do |vn, last, clk, expt, k, val|
            return unless @do_each_vn_dump
            yield vn_dump_pack(vn, last, clk, expt, k, val)
          end
        else
          # in case of out of :normal status
          @each_cache_lock.synchronize do
            each_unpacked_db(target_vn, @hdbc) do |cvn, clast, cclk, cexpt, k, cval|
              return unless @do_each_vn_dump
              data = @hdb[n].get(k)
              if data
                vn, last, clk, expt, val = unpack_data(data)
                #puts "#{k} #{clk} #{cclk} #{cmp_clk(clk, cclk)} #{val}"
                if cmp_clk(clk, cclk) > 0
                  yield vn_dump_pack(vn, last, clk, expt, k, val)
                else
                  yield vn_dump_pack(cvn, clast, cclk, cexpt, k, cval)
                end
              else
                yield vn_dump_pack(cvn, clast, cclk, cexpt, k, cval)
              end
            end
          end
          each_unpacked_db(target_vn, @hdb) do |vn, last, clk, expt, k, val|
            return unless @do_each_vn_dump
            unless @hdbc[n].get(k)
              yield vn_dump_pack(vn, last, clk, expt, k, val)
            end
          end
        end
      ensure
        @do_each_vn_dump = false
      end

      def vn_dump_pack(vn, last, clk, expt, k, val)
          if val
            return [vn, last, clk, expt, k.length, k, val.length, val].pack("NNNNNa#{k.length}Na#{val.length}")
          else
            return [vn, last, clk, expt, k.length, k, 0].pack("NNNNNa#{k.length}N")
          end          
      end
      private :vn_dump_pack

      def each_unpacked_db(target_vn, db)
        count = 0
        tn =  Time.now.to_i
        db[@hdiv[target_vn]].each do |k,v|
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
        @hdb[i].each{|k,v|
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

      # Remove a key for the cache(@hdbc).
      # +dn+:: number of database
      # +key+:: key
      def out_cache(dn, key)
        @hdbc[dn].out(key)
      end

      # Calls the geven block, 
      # passes the cache(@hdbc) element.
      # +dn+:: number of database
      # +keys+:: key list
      def each_cache_by_keys(dn, keys)
        keys.each do |k|
          v = @hdbc[dn].get(k)
          vn, last, clk, expt, val = unpack_data(v)
          yield [vn, last, clk, expt, k, val]
        end
      end

      # Calls the geven block, 
      # passes the cache(@hdbc) element as the spushv command data format.
      # +dn+:: number of database
      # +keys+:: key list
      def each_cache_dump_pack(dn, keys)
        keys.each do |k|
          v = @hdbc[dn].get(k)
          vn, last, clk, expt, val = unpack_data(v)
          yield vn_dump_pack(vn, last, clk, expt, k, val)
        end
      end

      # Returns a key array in a cache(@hdbc).
      # +dn+:: number of database
      # +kn+:: number of keys which is return value
      def get_keys_in_cache(dn, kn=100)
        return nil if @do_each_vn_dump
        ret = []
        return ret unless @hdbc[dn]
        count = 0
        @each_cache_lock.synchronize do
          @hdbc[dn].each do |k, v|
            ret << k
            break if (count+=1) >= kn
          end
        end
        ret
      end

      # Create vnode dump.
      def get_vnode_hash(vn)
        buf = {}
        count = 0
        @hdb[@hdiv[vn]].each{ |k, v|
          count += 1
          sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
          dat = unpack_data(v) #v.unpack('NNNN')
          buf[k] = v if dat[0] == vn
        }
        return buf
      end
      private :get_vnode_hash

      def flush_db(dn)
        @hdb[dn].sync
      end

      def set_db_stat(dn, stat)
        @stat_lock.synchronize do
          case @dbs[dn]
          when :normal
            if stat == :safecopy_flushing
              # open cache
              @hdbc[dn] = open_db("#{@storage_path}/#{dn}.cache.#{@ext_name}")
              stop_clean_up { @dbs[dn] = stat }
              stat
            else
              false
            end
          when :safecopy_flushing
            if stat == :safecopy_flushed
              @dbs[dn] = stat
            else
              false
            end
          when :safecopy_flushed
            if stat == :cachecleaning
              @dbs[dn] = stat
            else
              false
            end
          when :cachecleaning
            if stat == :normal
              @dbs[dn] = stat
              # remove cache
              close_db(@hdbc[dn])
              @hdbc[dn] = nil
              if File.exist?("#{@storage_path}/#{dn}.cache.#{@ext_name}")
                File.unlink("#{@storage_path}/#{dn}.cache.#{@ext_name}")
              end
              stat
            elsif stat == :safecopy_flushing
              @dbs[dn] = stat
            else
              false
            end
          else
            false
          end
        end
      end

    end # class BasicStorage

  end # module Storage
end # module Roma
