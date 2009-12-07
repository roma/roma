require 'digest/sha1'

module Roma
  module Storage

    class BasicStorage

      attr :hdb
      attr :hdiv
      attr :ext_name
      
      attr_reader :error_message

      attr_writer :vn_list
      attr_writer :storage_path
      attr_writer :divnum
      attr_writer :option

      attr_accessor :each_vn_dump_sleep
      attr_accessor :each_vn_dump_sleep_count
      attr_accessor :each_clean_up_sleep
      attr_accessor :logic_clock_expire

      def initialize
        @hdb = []
        @hdiv = Hash.new(0)

        @ext_name = 'db'

        @divnum = 10

        @each_vn_dump_sleep = 0.001
        @each_vn_dump_sleep_count = 100
        @each_clean_up_sleep = 0.01
        @logic_clock_expire = 300
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

      def opendb
        create_div_hash
        path = ''
        @storage_path.split('/').each{|p|
          if p.length==0
            path = '/'
            next
          end
          path << p
          Dir::mkdir(path) unless File.exist?(path)
          path << '/'
        }
        @divnum.times{ |i|
          @hdb[i] = open_db("#{@storage_path}/#{i}.#{@ext_name}")
        }
      end

      def closedb
        buf = @hdb; @hdb = []
        buf.each{ |hdb| close_db(hdb) }
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


      def get_context(vn, k, d)
        buf = @hdb[@hdiv[vn]].get(k)
        return nil unless buf
        unpack_header(buf)
      end

      def rset(vn, k, d, clk, expt, v)
        buf = @hdb[@hdiv[vn]].get(k)
        t = Time.now.to_i
        if buf
          data = unpack_data(buf)
          if t - data[1] < @logic_clock_expire && cmp_clk(clk,data[2]) <= 0
            @error_message = "error:#{t-data[1]} < #{@logic_clock_expire} && cmp_clk(#{clk},#{data[2]})<=0"
            return nil
          end
        end

        ret = [vn, t, clk, expt, v]
        return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        @error_message = "error:put"
        nil
      end

      def set(vn, k, d, expt, v)
        buf = @hdb[@hdiv[vn]].get(k)
        clk = 0
        if buf
          data = unpack_data(buf)
          clk = (data[2] + 1) & 0xffffffff
        end

        ret = [vn, Time.now.to_i, clk, expt, v]
        return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        nil
      end

      def add(vn, k, d, expt, v)
        buf = @hdb[@hdiv[vn]].get(k)
        clk = 0
        if buf
          vn, t, clk, expt2, v2 = unpack_data(buf)
          return nil if Time.now.to_i <= expt2
          clk = (clk + 1) & 0xffffffff
        end
        
        # not exist
        ret = [vn, Time.now.to_i, clk, expt, v]
        return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        nil
      end

      def replace(vn, k, d, expt, v)
        buf = @hdb[@hdiv[vn]].get(k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        ret = [vn, Time.now.to_i, clk, expt, v]
        return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        nil
      end

      def append(vn, k, d, expt, v)
        buf = @hdb[@hdiv[vn]].get(k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        ret = [vn, Time.now.to_i, clk, expt, v2 + v]
        return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        nil
      end

      def prepend(vn, k, d, expt, v)
        buf = @hdb[@hdiv[vn]].get(k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        ret = [vn, Time.now.to_i, clk, expt, v + v2]
        return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        nil
      end

      def get(vn, k, d)
        buf = @hdb[@hdiv[vn]].get(k)
        return nil unless buf
        vn, t, clk, expt, v = unpack_data(buf)

        return nil if Time.now.to_i > expt
        v
      end

      def get_raw(vn, k, d)
        buf = @hdb[@hdiv[vn]].get(k)
        return nil unless buf

        unpack_data(buf)
      end

      def rdelete(vn, k, d, clk)
        buf = @hdb[@hdiv[vn]].get(k)
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
        if @hdb[@hdiv[vn]].put(k,pack_header(*ret))
          return ret
        else
          return nil
        end
      end

      def delete(vn, k, d)
        buf = @hdb[@hdiv[vn]].get(k)
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
        if @hdb[@hdiv[vn]].put(k,pack_header(*ret[0..-2]))
          return ret
        else
          return nil
        end
      end

      def out(vn, k, d)
        @hdb[@hdiv[vn]].out(k)
      end 

      def incr(vn, k, d, v)
        buf = @hdb[@hdiv[vn]].get(k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        v = (v2.to_i + v)
        v = 0 if v < 0
        v = v & 0xffffffffffffffff

        ret = [vn, Time.now.to_i, clk, expt2, v.to_s]
        return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        nil
      end

      def decr(vn, k, d, v)
        buf = @hdb[@hdiv[vn]].get(k)
        return nil unless buf

        # buf != nil
        vn, t, clk, expt2, v2 = unpack_data(buf)
        return nil if Time.now.to_i > expt2
        clk = (clk + 1) & 0xffffffff

        v = (v2.to_i - v)
        v = 0 if v < 0
        v = v & 0xffffffffffffffff

        ret = [vn, Time.now.to_i, clk, expt2, v.to_s]
        return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        nil
      end

      def true_length
        res = 0
        @hdb.each{ |hdb| res += hdb.rnum }
        res
      end

      def add_vnode(vn)
      end

      def del_vnode(vn)
        buf = get_vnode_hash(vn)
        buf.each_key{ |k| @hdb[@hdiv[vn]].out(k) }
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
              #delkey << k
              @hdb[i].out(k)
            end
            if unit_test_flg
              closedb
            end
            sleep @each_clean_up_sleep
          }
          #delkey.each{ |k| @hdb[i].out(k) }
        }
        n
      rescue => e
        raise NoMethodError(e.message)
      end

      def each_clean_up(t, vnhash)
        @do_clean_up = true
        nt = Time.now.to_i
        @hdb.each{ |hdb|
          hdb.each{ |k, v|
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

      def stop_clean_up
         @do_clean_up = false
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
        buf = @hdb[@hdiv[vn]].get(k)
        if buf
          data = unpack_header(buf)
          if last - data[1] < @logic_clock_expire && cmp_clk(clk,data[2]) <= 0
            return nil
          end
        end
        
        ret = [vn, last, clk, expt, v]
        if expt == 0
          return ret if @hdb[@hdiv[vn]].put(k, pack_header(*ret[0..3]))
        else
          return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        end
        nil
      end

      # Returns the vnode dump.
      def dump(vn)
        buf = get_vnode_hash(vn)
        return nil if buf.length == 0
        Marshal.dump(buf)
      end

      def dump_file(path,except_vnh = nil)
        pbuf = ''
        path.split('/').each{|p|
          pbuf << p
          begin
            Dir::mkdir(pbuf) unless File.exist?(pbuf)
          rescue
          end
          pbuf << '/'
        }
        @divnum.times{|i|
          f = open("#{path}/#{i}.dump","wb")
          each_hdb_dump(i,except_vnh){|data| f.write(data) }
          f.close
        }
        open("#{path}/eod","w"){|f|
          f.puts Time.now
        }
      end

      def each_vn_dump(target_vn)
        count = 0
        @divnum.times{|i|
          tn =  Time.now.to_i
          @hdb[i].each{|k,v|
            vn, last, clk, expt, val = unpack_data(v)
            if vn != target_vn || (expt != 0 && tn > expt)
              count += 1              
              sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
              next
            end
            if val
              yield [vn, last, clk, expt, k.length, k, val.length, val].pack("NNNNNa#{k.length}Na#{val.length}")
            else
              yield [vn, last, clk, expt, k.length, k, 0].pack("NNNNNa#{k.length}N")
            end
          }
        }
      end

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
      private :each_hdb_dump

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

    end # class BasicStorage

  end # module Storage
end # module Roma
