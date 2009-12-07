require 'zlib'
require 'digest/sha1'
require "roma/config"
require 'roma/async_process'

module Roma
  module Command

    module StorageCommandReceiver

      # "set" means "store this data".
      # <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
      # <data block>\r\n
      def ev_set(s); set(:set,s); end
      def ev_fset(s); fset(:set,s); end

      # rset <key> <hash value> <timelimit> <length>
      # "set" means "store this data".
      # <command name> <key> <digest> <exptime> <bytes> [noreply]\r\n
      # <data block>\r\n
      def ev_rset(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = s[2].to_i
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits if d == 0
        data = read_bytes(s[5].to_i)
        read_bytes(2)
        vn = @rttable.get_vnode_id(d)
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end
        if @storages[hname].rset(vn, key, d, s[3].to_i, s[4].to_i, data)
          send_data("STORED\r\n")
        else
          @log.error("rset NOT_STORED:#{@storages[hname].error_message} #{vn} #{s[1]} #{d} #{s[3]} #{s[4]}")
          send_data("NOT_STORED\r\n")
        end
        @stats.redundant_count += 1
      end

      # <command name> <key> <digest> <exptime> <bytes> [noreply]\r\n
      # <compressed data block>\r\n
      def ev_rzset(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = s[2].to_i
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits if d == 0
        zdata = read_bytes(s[5].to_i)
        read_bytes(2)
        vn = @rttable.get_vnode_id(d)
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end

        data = Zlib::Inflate.inflate(zdata)
        if @storages[hname].rset(vn, key, d, s[3].to_i, s[4].to_i, data)
          send_data("STORED\r\n")
        else
          @log.error("rzset NOT_STORED:#{@storages[hname].error_message} #{vn} #{s[1]} #{d} #{s[3]} #{s[4]}")
          send_data("NOT_STORED\r\n")
        end
        @stats.redundant_count += 1
      rescue Zlib::DataError => e
        @log.error("rzset NOT_STORED:#{e} #{vn} #{s[1]} #{d} #{s[3]} #{s[4]}")
        send_data("NOT_STORED\r\n")
      end

      # get <key>*\r\n
      def ev_get(s)
        return ev_gets(s) if s.length > 2

        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        vn = @rttable.get_vnode_id(d)
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end
        data = @storages[hname].get(vn, key, 0)
        @stats.read_count += 1
        if data
          return send_data("VALUE #{s[1]} 0 #{data.length}\r\n#{data}\r\nEND\r\n")
        end

        nodes = @rttable.search_nodes(vn)
        if nodes.include?(@nid)
          return send_data("END\r\n")
        end

        nodes.delete(@nid)
        if nodes.length != 0
          @log.warn("forward get #{s[1]}")
          res = forward_get(nodes[0], s[1], d)
          if res
            return send_data(res)
          end
        end
        send_data("SERVER_ERROR Message forward failed.\r\n")
      end

      # gets <key>*\r\n
      def ev_gets(s)
        nk = {} # {node-id1=>[key1,key2,..],node-id2=>[key3,key4,..]}
        kvn = {} # {key1=>vn1, key2=>vn2, ... }
        s[1..-1].each{|kh|
          key, = kh.split("\e") # split a hash-name
          d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
          kvn[key] = vn = @rttable.get_vnode_id(d)
          nodes = @rttable.search_nodes(vn)
          unless nodes.empty? # check the node existence
            nk[nodes[0]]=[] unless nk.key?(nodes[0])
            nk[nodes[0]] << kh
          end
        }

        res = {} # result data {key1=>val1,key2=>val2,...}
        if nk.key?(@nid)
          nk[@nid].each{|kh|
            key,hname = kh.split("\e")
            hname ||= @defhash
            if @storages.key?(hname)
              vn, t, clk, expt, val = @storages[hname].get_raw(kvn[key], key, 0)
              @stats.read_count += 1
              res[key] = [clk, val] if val && Time.now.to_i <= expt
            end
          }
          nk.delete(@nid)
        end

        nk.each_pair{|nid,keys|
          res.merge!(forward_gets(nid,keys))
        }

        res.each_pair{|key,cv|
          clk, val = cv
          send_data("VALUE #{key} 0 #{val.length} #{clk}\r\n#{val}\r\n")
        }
        send_data("END\r\n")
      end


      # delete <key> [<time>] [noreply]\r\n
      def ev_delete(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes[0] != @nid
          cmd = "fdelete #{s[1]}"
          s[2..-1].each{|c| cmd << " #{c}"}
          cmd << "\r\n"
          @log.warn("forward delete #{s[1]}")
          res = send_cmd(nodes[0], cmd)
          if res
            return send_data("#{res}\r\n")
          end
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        end
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end
        res = @storages[hname].delete(vn, key, d)
        @stats.delete_count += 1
        return send_data("NOT_DELETED\r\n") unless res
        return send_data("NOT_FOUND\r\n") if res == :deletemark

        nodes[1..-1].each{ |nid|
          send_cmd(nid,"rdelete #{s[1]} #{res[2]}\r\n")
        }
        return send_data("NOT_FOUND\r\n") unless res[4]
        send_data("DELETED\r\n")
      end

      # fdelete <key> [<time>] [noreply]\r\n
      def ev_fdelete(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes.include?(@nid) == false
          @log.error("fdelete failed delete key=#{s[1]} vn=#{vn}")
          return send_data("SERVER_ERROR Routing table is inconsistent.\r\n")
        end
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end
        res = @storages[hname].delete(vn, key, d)
        @stats.delete_count += 1
        return send_data("NOT_DELETED\r\n") unless res
        return send_data("NOT_FOUND\r\n") if res == :deletemark

        nodes.delete(@nid)
        nodes.each{ |nid|
          send_cmd(nid,"rdelete #{s[1]} #{res[2]}\r\n")
        }
        return send_data("NOT_FOUND\r\n") unless res[4]
        send_data("DELETED\r\n")
      end

      # rdelete <key> <clock>
      def ev_rdelete(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        vn = @rttable.get_vnode_id(d)
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end
        if @storages[hname].rdelete(vn, key, d, s[2].to_i)
          send_data("DELETED\r\n")
        else
          send_data("NOT_FOUND\r\n")
        end
      end

      # out <key> <vn>
      def ev_out(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        if s.length >= 3
          vn = s[2].to_i
        else
          d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
          vn = @rttable.get_vnode_id(d)
        end
        res = @storages[hname].out(vn, key, 0)
        @stats.out_message_count += 1
        unless res
          return send_data("NOT_DELETED\r\n")
        end
        send_data("DELETED\r\n")
      end

      # "add" means that "add a new data to a store"
      # <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
      # <data block>\r\n
      def ev_add(s); set(:add,s); end
      def ev_fadd(s); fset(:add,s); end

      # "replace" means that "replace the previous data with a new one"
      # <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
      # <data block>\r\n
      def ev_replace(s); set(:replace,s); end
      def ev_freplace(s); fset(:replace,s); end

      # "append" means that "append a new data to the previous one"
      # <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
      # <data block>\r\n
      def ev_append(s); set(:append,s); end
      def ev_fappend(s); fset(:append,s); end

      # "prepend" means that "prepend a new data to the previous one"
      # <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
      # <data block>\r\n
      def ev_prepend(s); set(:prepend,s); end
      def ev_fprepend(s); fset(:prepend,s); end


      # "cas" means that "store this data but only if no one else has updated since I last fetched it."
      # <command name> <key> <flags> <exptime> <bytes> <cas-id>[noreply]\r\n
      # <data block>\r\n
      def ev_cas(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        v = read_bytes(s[4].to_i)
        read_bytes(2)
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes[0] != @nid
          @log.warn("forward cas key=#{key} vn=#{vn} to #{nodes[0]}")
          res = send_cmd(nodes[0],"fcas #{s[1]} #{d} #{s[3]} #{v.length} #{s[5]}\r\n#{v}\r\n")
          if res
            return send_data("#{res}\r\n")
          end
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        end

        store_cas(hname, vn, key, d, s[5].to_i, s[3].to_i, v, nodes[1..-1])
      end

      def ev_fcas(s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = s[2].to_i
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits if d == 0
        v = read_bytes(s[4].to_i)
        read_bytes(2)
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes.include?(@nid) == false
          @log.error("fcas failed key = #{s[1]} vn = #{vn}")
          return send_data("SERVER_ERROR Routing table is inconsistent.\r\n")
        end

        nodes.delete(@nid)
        store_cas(hname, vn, key, d, s[5].to_i, s[3].to_i, v, nodes)
      end

      # incr <key> <value> [noreply]\r\n
      def ev_incr(s); incr_decr(:incr,s); end
      def ev_fincr(s); fincr_fdecr(:incr,s); end

      # decr <key> <value> [noreply]\r\n
      def ev_decr(s); incr_decr(:decr,s); end
      def ev_fdecr(s); fincr_fdecr(:decr,s); end

      # set_size_of_zredundant <n>
      def ev_set_size_of_zredundant(s)
        if s.length != 2 || s[1].to_i == 0
          return send_data("usage:set_set_size_of_zredundant <n>\r\n")
        end
        res = broadcast_cmd("rset_size_of_zredundant #{s[1]}\r\n")
        @stats.size_of_zredundant = s[1].to_i
        res[@stats.ap_str] = "STORED"
        send_data("#{res}\r\n")
      end

      # rset_size_of_zredundant <n>
      def ev_rset_size_of_zredundant(s)
        if s.length != 2 || s[1].to_i == 0
          return send_data("usage:set_set_size_of_zredundant <n>\r\n")
        end
        @stats.size_of_zredundant = s[1].to_i
        send_data("STORED\r\n")
      end

      private

      def forward_get(nid, k, d)
        con = get_connection(nid)
        con.send("get #{k}\r\n")
        res = con.gets
        return res if res == "END\r\n"
        s = res.split(/ /)
        res << con.read_bytes(s[3].to_i + 2)
        res << con.gets
        return_connection(nid, con)
        @rttable.proc_succeed(nid)
        res
      rescue => e
        @rttable.proc_failed(nid)
        @log.error("forward get failed:nid=#{nid} key=#{key}")
        nil
      end

      def forward_gets(nid, keys)
        con = get_connection(nid)
        con.send("gets #{keys.join(' ')}\r\n")
        res = {}
        while((line = con.gets)!="END\r\n")
          s = line.chomp.split(/ /)
          res[s[1]] = [s[4], con.read_bytes(s[3].to_i)]
          con.read_bytes(2)
        end
        return_connection(nid, con)
        @rttable.proc_succeed(nid)
        res
      rescue => e
        @rttable.proc_failed(nid)
        @log.error("forward gets failed:nid=#{nid} key=#{keys}")
        nil
      end

      def store(fnc, hname, vn, k, d, expt, v, nodes)
        if expt == 0
          expt = 0x7fffffff
        elsif expt < 2592000
          expt += Time.now.to_i
        end
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end
        ret = @storages[hname].send(fnc, vn, k, d, expt ,v)
        @stats.write_count += 1
        if ret
          redundant(nodes, hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          @log.error("#{fnc} NOT_STORED:#{hname} #{vn} #{k} #{d} #{expt}")
          send_data("NOT_STORED\r\n")
        end
      end

      def store_cas(hname, vn, k, d, clk, expt, v, nodes)
        if expt == 0
          expt = 0x7fffffff
        elsif expt < 2592000
          expt += Time.now.to_i
        end
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end

        ret = @storages[hname].cas(vn, k, d, clk, expt ,v)
        @stats.write_count += 1
        case ret
        when nil
          @log.error("cas NOT_STORED:#{hname} #{vn} #{k} #{d} #{expt} #{clk}")
          send_data("NOT_STORED\r\n")
        when :not_found
          send_data("NOT_FOUND\r\n")
        when :exists
          send_data("EXISTS\r\n")
        else
          redundant(nodes, hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")          
        end
      end

      def redundant(nodes, hname, k, d, clk, expt, v)
        if @rttable.min_version == nil || @rttable.min_version < 0x000306 # ver.0.3.6
          return redundant_older_than_000306(nodes, hname, k, d, clk, expt, v)
        end

        if @stats.size_of_zredundant > 0 && @stats.size_of_zredundant < v.length 
          return zredundant(nodes, hname, k, d, clk, expt, v)
        end

        nodes.each{ |nid|
          res = send_cmd(nid,"rset #{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}\r\n#{v}\r\n")
          unless res
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('redundant',[nid,hname,k,d,clk,expt,v]))
            @log.warn("redundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
          end
        }
      end

      def redundant_older_than_000306(nodes, hname, k, d, clk, expt, v)
        nodes.each{ |nid|
          if @rttable.version_of_nodes[nid] >= 0x000306 &&
              @stats.size_of_zredundant > 0 && @stats.size_of_zredundant < v.length

            zv = Zlib::Deflate.deflate(v) unless zv
            res = send_cmd(nid,"rzset #{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}\r\n#{zv}\r\n")
            unless res
              Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('zredundant',[nid,hname,k,d,clk,expt,zv]))
              @log.warn("redundant_older_than_000306 failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length} -> #{nid}")
            end
          else
            res = send_cmd(nid,"rset #{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}\r\n#{v}\r\n")
            unless res
              Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('redundant',[nid,hname,k,d,clk,expt,v]))
              @log.warn("redundant_older_than_000306 failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
            end
          end
        }
      end

      def zredundant(nodes, hname, k, d, clk, expt, v)
        zv = Zlib::Deflate.deflate(v)

        nodes.each{ |nid|
          res = send_cmd(nid,"rzset #{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}\r\n#{zv}\r\n")
          unless res
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('zredundant',[nid,hname,k,d,clk,expt,zv]))
            @log.warn("zredundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length} -> #{nid}")
          end
        }
      end

      def set(fnc,s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        v = read_bytes(s[4].to_i)
        read_bytes(2)
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes[0] != @nid
          @log.warn("forward #{fnc} key=#{key} vn=#{vn} to #{nodes[0]}")
          res = send_cmd(nodes[0],"f#{fnc} #{s[1]} #{d} #{s[3]} #{v.length}\r\n#{v}\r\n")
          if res
            return send_data("#{res}\r\n")
          end
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        end

        store(fnc, hname, vn, key, d, s[3].to_i, v, nodes[1..-1])
      end

      def fset(fnc,s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = s[2].to_i
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits if d == 0
        v = read_bytes(s[4].to_i)
        read_bytes(2)
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes.include?(@nid) == false
          @log.error("f#{fnc} failed key = #{s[1]} vn = #{vn}")
          return send_data("SERVER_ERROR Routing table is inconsistent.\r\n")
        end

        nodes.delete(@nid)
        store(fnc, hname, vn, key, d, s[3].to_i, v, nodes)
      end

      def store_incr_decr(fnc, hname, vn, k, d,  v, nodes)
        unless @storages.key?(hname)
          send_data("SERVER_ERROR #{hname} dose not exists.\r\n")
          return
        end
        res = @storages[hname].send(fnc, vn, k, d, v)
        @stats.write_count += 1
        if res
          redundant(nodes, hname, k, d, res[2], res[3], res[4])
          send_data("#{res[4]}\r\n")
        else
          send_data("NOT_FOUND\r\n")
        end
      end

      def incr_decr(fnc,s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
        v = s[2].to_i
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes[0] != @nid
          @log.debug("forward #{fnc} key=#{s[1]} vn=#{vn} to #{nodes[0]}")
          res = send_cmd(nodes[0],"f#{fnc} #{s[1]} #{d} #{s[2]}\r\n")
          if res
            return send_data("#{res}\r\n")
          end
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        end

        store_incr_decr(fnc, hname, vn, key, d, v, nodes[1..-1])
      end

      def fincr_fdecr(fnc,s)
        key,hname = s[1].split("\e")
        hname ||= @defhash
        d = s[2].to_i
        d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits if d == 0
        v = s[3].to_i
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        if nodes.include?(@nid) == false
          @log.debug("f#{fnc} failed key = #{s[1]} vn = #{vn}")
          return send_data("SERVER_ERROR Routing table is inconsistent.\r\n")
        end
        
        nodes.delete(@nid)
        store_incr_decr(fnc, hname, vn, key, d, v, nodes)
      end

    end # module StorageCommandReceiver

  end # module Command
end # module Roma
