require 'timeout'
require 'roma/messaging/con_pool'
require 'json'

module Roma
  module CommandPlugin

    module PluginAshiatoList
      include ::Roma::CommandPlugin

      # alist_at <key> <index> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_at(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s)  if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1
        if ddata
          v = Marshal.load(ddata)[0]
          return send_data("END\r\n") if v.length <= s[2].to_i
          ret = v.at(s[2].to_i)
          ret = "" unless ret
          return send_data("VALUE #{s[1]} 0 #{ret.length}\r\n#{ret}\r\nEND\r\n")
        else
          return send_data("END\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_clear <key> [forward]\r\n
      #
      # (CLEARED|NOT_CLEARED|SERVER_ERROR <error message>)\r\n
      def ev_alist_clear(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward2(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        return send_data("NOT_FOUND\r\n") unless ddata

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump([[],[]]))
        @stats.delete_count += 1


        if ret
          if @stats.wb_command_map.key?(:alist_clear)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_clear], k, ddata)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("CLEARED\r\n")
        else
          send_data("NOT_CLEARED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_delete <key> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (DELETED|NOT_DELETED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def ev_alist_delete(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[2].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        return send_data("NOT_FOUND\r\n") unless ddata

        v = Marshal.load(ddata)
        return send_data("NOT_DELETED\r\n") unless v[0].include?(data)
        while(idx = v[0].index(data))
          v[0].delete_at(idx)
          v[1].delete_at(idx)
        end

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.delete_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_delete)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_delete], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("DELETED\r\n")
        else
          send_data("NOT_DELETED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_delete_at <key> <index> [forward]\r\n
      #
      # (DELETED|NOT_DELETED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def ev_alist_delete_at(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward2(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        return send_data("NOT_FOUND\r\n") unless ddata

        v = Marshal.load(ddata)
        dret = v[0].delete_at(s[2].to_i)
        return send_data("NOT_DELETED\r\n") unless dret
        v[1].delete_at(s[2].to_i)

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.delete_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_delete_at)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_delete_at], k, dret)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("DELETED\r\n")
        else
          send_data("NOT_DELETED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("${e} #{$@}")
      end

      # alist_empty? <key> [forward]\r\n
      #
      # (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def ev_alist_empty?(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward2(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        return send_data("NOT_FOUND\r\n") unless ddata

        v = Marshal.load(ddata)
        ret = v[0].empty?

        send_data("#{ret}\r\n")
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_first <key> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_first(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        if ddata
          v = Marshal.load(ddata)[0]
          return send_data("END\r\n") if v.length == 0
          ret = v.first
          return send_data("VALUE #{s[1]} 0 #{ret.length}\r\n#{ret}\r\nEND\r\n")
        else
          return send_data("END\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_gets <key> [index|range] [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <length of length string>\r\n
      # <length string>\r\n
      # (VALUE <key> 0 <value length>\r\n
      # <value>\r\n)*
      # ]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_gets(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, 0)
        @stats.read_count += 1

        if ddata
          v = Marshal.load(ddata)[0]
          if /(?:^(\d+)$|^(\d+)..((?:-)?\d+)$)/ =~ s[2]
            if $1
              if v.length <= $1.to_i
                return send_data("END\r\n")
              end
              buf = v[Range.new($1.to_i,$1.to_i)]
            else
              buf = v[Range.new($2.to_i,$3.to_i)]
            end
          else
            buf = v
          end
          len = v.length
          send_data("VALUE #{s[1]} 0 #{len.to_s.length}\r\n#{len.to_s}\r\n")
          buf.each{|val|
            send_data("VALUE #{s[1]} 0 #{val.length}\r\n#{val}\r\n")
          }
          return send_data("END\r\n")
        else
          return send_data("END\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_gets_with_time <key> [index|range] [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <length of length string>\r\n
      # <length string>\r\n
      # (VALUE <key> 0 <value length>\r\n
      # <value string>\r\n
      # VALUE <key> 0 <value length>\r\n
      # <time string>\r\n)*
      # ]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_gets_with_time(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, 0)
        @stats.read_count += 1

        if ddata
          v = Marshal.load(ddata)
          if /(?:^(\d+)$|^(\d+)..((?:-)?\d+)$)/ =~ s[2]
            if $1
              if v[0].length <= $1.to_i
                return send_data("END\r\n")
              end
              v_buf = v[0][Range.new($1.to_i,$1.to_i)]
              t_buf = v[1][Range.new($1.to_i,$1.to_i)]
            else
              v_buf = v[0][Range.new($2.to_i,$3.to_i)]
              t_buf = v[1][Range.new($2.to_i,$3.to_i)]
            end
          else
            v_buf = v[0]
            t_buf = v[1]
          end
          len = v[0].length
          send_data("VALUE #{s[1]} 0 #{len.to_s.length}\r\n#{len.to_s}\r\n")
          v_buf.each_with_index{|val,idx|
            send_data("VALUE #{s[1]} 0 #{val.length}\r\n#{val}\r\n")
            send_data("VALUE #{s[1]} 0 #{t_buf[idx].to_s.length}\r\n#{t_buf[idx]}\r\n")
          }
          return send_data("END\r\n")
        else
          return send_data("END\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end


      # alist_include? <key> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def ev_alist_include?(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[2].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        return send_data("NOT_FOUND\r\n") unless ddata

        v = Marshal.load(ddata)[0]
        ret = v.include?(data)

        send_data("#{ret}\r\n")
      rescue => e
        send_data("SERVER_ERROR #{e} #{$@}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_index <key> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (<index>|nil|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def ev_alist_index(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[2].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        return send_data("NOT_FOUND\r\n") unless ddata

        v = Marshal.load(ddata)[0]
        ret = v.index(data)
        if ret
          send_data("#{ret}\r\n")
        else
          send_data("nil\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_insert <key> <index> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_insert(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[3].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
        else
          v = [[],[]]
        end

        v[0].insert(s[2].to_i,data)
        v[1].insert(s[2].to_i,Time.now.to_i)
        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_insert)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_insert], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      def ev_alist_sized_prepend(s); ev_alist_sized_insert(s); end

      # alist_sized_insert <key> <array-size> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_sized_insert(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[3].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
        else
          v = [[],[]]
        end

        v[0].insert(0,data)
        v[0] = v[0][0..(s[2].to_i - 1)]
        v[1].insert(0,Time.now.to_i)
        v[1] = v[1][0..(s[2].to_i - 1)]

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_sized_insert)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_sized_insert], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      def ev_alist_delete_and_prepend(s); ev_alist_swap_and_insert(s); end

      # alist_swap_and_insert <key> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_swap_and_insert(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[2].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
        else
          v = [[],[]]
        end

        idx = v[0].index(data)
        if idx
          v[0].delete_at(idx)
          v[1].delete_at(idx)
        end
        v[0].insert(0,data)
        v[1].insert(0,Time.now.to_i)

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_swap_and_insert)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_swap_and_insert], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      def ev_alist_sized_delete_and_prepend(s); ev_alist_swap_and_sized_insert(s); end

      # alist_swap_and_sized_insert <key> <array-size> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_swap_and_sized_insert(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[3].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
        else
          v = [[],[]]
        end

        idx = v[0].index(data)
        if idx
          v[0].delete_at(idx)
          v[1].delete_at(idx)
        end
        v[0].insert(0,data)
        v[1].insert(0,Time.now.to_i)
        v[0] = v[0][0..(s[2].to_i - 1)]
        v[1] = v[1][0..(s[2].to_i - 1)]

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_swap_and_sized_insert)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_swap_and_sized_insert], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_expired_swap_and_insert <key> <expire-time> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # the data expire-time's ago will be deleated.
      # the unit of the expire-time's is a second.
      # however,as follows when there is a suffix.
      # 'h' as +expire-time+ suffix is hour.
      # 'd' as +expire-time+ suffix is day.
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_expired_swap_and_insert(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[3].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        et = expired_str_to_i(s[2])
        return send_data("SERVER_ERROR format error in expire-time.\r\n") unless et

        v = to_alist_value_for_write(hname, vn, k, d)
        unless v
          return send_data("SERVER_ERROR data other than alist's format already exist.\r\n")
        end

# @logger.debug("#{s[2]} et=#{et}")
        v = expired_swap(v, data, et)

        v[0].insert(0,data)
        v[1].insert(0,Time.now.to_i)

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_expired_swap_and_insert)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_expired_swap_and_insert], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_expired_swap_and_sized_insert <key> <expire-time> <array-size> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # the data expire-time's ago will be deleated.
      # the unit of the expire-time's is a second.
      # however,as follows when there is a suffix.
      # 'h' as +expire-time+ suffix is hour.
      # 'd' as +expire-time+ suffix is day.
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_expired_swap_and_sized_insert(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[4].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        et = expired_str_to_i(s[2])
        return send_data("SERVER_ERROR format error in expire-time.\r\n") unless et

        v = to_alist_value_for_write(hname, vn, k, d)
        unless v
          return send_data("SERVER_ERROR data other than alist's format already exist.\r\n")
        end

# @logger.debug("#{s[2]} et=#{et}")
        v = expired_swap(v, data, et)

        v[0].insert(0,data)
        v[0] = v[0][0..(s[3].to_i - 1)]
        v[1].insert(0,Time.now.to_i)
        v[1] = v[1][0..(s[3].to_i - 1)]

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_expired_swap_and_sized_insert)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_expired_swap_and_sized_insert], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end


      # alist_join_with_time <key> <bytes> [index|range] [forward]\r\n
      # <separator block>\r\n
      #
      # (
      # [VALUE <key> 0 <length of length string>\r\n
      # <length string>\r\n
      # VALUE <key> 0 <value length>\r\n
      # <value string>\r\n
      # VALUE <key> 0 <value length>\r\n
      # <time string>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_join_with_time(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[2].to_i)
        read_bytes(2)
        return forward1(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, 0)
        @stats.read_count += 1
        if ddata
          v = Marshal.load(ddata)
          if /(?:^(\d+)$|^(\d+)..((?:-)?\d+)$)/ =~ s[3]
            if $1
              if v[0].length <= $1.to_i
                return send_data("END\r\n")
              end
              v_buf = v[0][Range.new($1.to_i,$1.to_i)]
              t_buf = v[1][Range.new($1.to_i,$1.to_i)]
            else
              v_buf = v[0][Range.new($2.to_i,$3.to_i)]
              t_buf = v[1][Range.new($2.to_i,$3.to_i)]
            end
          else
            v_buf = v[0]
            t_buf = v[1]
          end
          len = v[0].length
          v_ret = v_buf.join(data)
          t_ret = t_buf.join(data)
          send_data("VALUE #{s[1]} 0 #{len.to_s.length}\r\n#{len.to_s}\r\n")
          send_data("VALUE #{s[1]} 0 #{v_ret.length}\r\n#{v_ret}\r\n")
          return send_data("VALUE #{s[1]} 0 #{t_ret.length}\r\n#{t_ret}\r\nEND\r\n")
        else
          return send_data("END\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_join <key> <bytes> [index|range] [forward]\r\n
      # <separator block>\r\n
      #
      # (
      # [VALUE <key> 0 <length of length string>\r\n
      # <length string>\r\n
      # VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_join(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[2].to_i)
        read_bytes(2)
        return forward1(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, 0)
        @stats.read_count += 1

        if ddata
          v = Marshal.load(ddata)[0]
          if /(?:^(\d+)$|^(\d+)..((?:-)?\d+)$)/ =~ s[3]
            if $1
              if v.length <= $1.to_i
                return send_data("END\r\n")
              end
              buf = v[Range.new($1.to_i,$1.to_i)]
            else
              buf = v[Range.new($2.to_i,$3.to_i)]
            end
          else
            buf = v
          end
          len = v.length
          ret = buf.join(data)
          send_data("VALUE #{s[1]} 0 #{len.to_s.length}\r\n#{len.to_s}\r\n")
          return send_data("VALUE #{s[1]} 0 #{ret.length}\r\n#{ret}\r\nEND\r\n")
        else
          return send_data("END\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_to_json <key> [index|range] [forward]\r\n
      #
      # (
      # VALUE <key> 0 <length of json string>\r\n
      # <json string>\r\n
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_to_json(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, 0)
        @stats.read_count += 1

        if ddata
          v = Marshal.load(ddata)[0]
          ret = nil
          if /(?:^(\d+)$|^(\d+)..((?:-)?\d+)$)/ =~ s[2]
            if $1
              if v.length <= $1.to_i
                return send_data("END\r\n")
              end
              ret = JSON.generate(v[Range.new($1.to_i,$1.to_i)])
            else
              ret = JSON.generate(v[Range.new($2.to_i,$3.to_i)])
            end
          else
            ret = JSON.generate(v)
          end
          return send_data("VALUE #{s[1]} 0 #{ret.length}\r\n#{ret}\r\nEND\r\n")
        else
          return send_data("END\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_last <key> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_last(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        if ddata
          v = Marshal.load(ddata)[0]
          return send_data("END\r\n") if v.length == 0
          ret = v.last
          return send_data("VALUE #{s[1]} 0 #{ret.length}\r\n#{ret}\r\nEND\r\n")
        else
          return send_data("END\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_length <key> [forward]\r\n
      #
      # (<length>|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def ev_alist_length(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward2(nodes[0], s) if nodes[0] != @nid
        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        return send_data("NOT_FOUND\r\n") unless ddata
        v = Marshal.load(ddata)[0]
        ret = v.length
        send_data("#{ret}\r\n")
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_pop <key> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END
      # |NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_pop(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
          return send_data("END\r\n") if v[0].length ==0
        else
          return send_data("END\r\n")
        end

        retv = v[0].pop
        v[1].pop
        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.read_count += 1
        @stats.write_count += 1

        if ret
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("VALUE #{s[1]} 0 #{retv.length}\r\n#{retv}\r\nEND\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_push <key> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_push(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[2].to_i)
        read_bytes(2)
        if nodes[0] != @nid
          return forward2(nodes[0], s, data)
        end

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
        else
          v = [[],[]]
        end

        v[0].push(data)
        v[1].push(Time.now.to_i)
        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_push)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_push], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_sized_push <key> <array-size> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_PUSHED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_sized_push(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[3].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
        else
          v = [[],[]]
        end

        max = s[2].to_i
        return send_data("NOT_PUSHED\r\n") if v[0].length >= max

        v[0].push(data)
        v[0] = v[0][0..(max - 1)]
        v[1].push(Time.now.to_i)
        v[1] = v[1][0..(max - 1)]

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1


        if ret
          if @stats.wb_command_map.key?(:alist_sized_push)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_sized_push], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_swap_and_push <key> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_swap_and_push(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[2].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
        else
          v = [[],[]]
        end

        idx = v[0].index(data)
        if idx
          v[0].delete_at(idx)
          v[1].delete_at(idx)
        end
        v[0].push(data)
        v[1].push(Time.now.to_i)

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_swap_and_push)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_swap_and_push], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_swap_and_sized_push <key> <array-size> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_PUSHED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_swap_and_sized_push(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[3].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
        else
          v = [[],[]]
        end

        max = s[2].to_i

        idx = v[0].index(data)
        if idx
          v[0].delete_at(idx)
          v[1].delete_at(idx)
        else
          return send_data("NOT_PUSHED\r\n") if v[0].length >= max
        end
        v[0].push(data)
        v[0] = v[0][0..(max - 1)]
        v[1].push(Time.now.to_i)
        v[1] = v[1][0..(max - 1)]

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_swap_and_sized_push)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_swap_and_sized_push], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_expired_swap_and_push <key> <expire-time> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # the data expire-time's ago will be deleated.
      # the unit of the expire-time's is a second.
      # however,as follows when there is a suffix.
      # 'h' as +expire-time+ suffix is hour.
      # 'd' as +expire-time+ suffix is day.
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_expired_swap_and_push(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[3].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        et = expired_str_to_i(s[2])
        return send_data("SERVER_ERROR format error in expire-time.\r\n") unless et

        v = to_alist_value_for_write(hname, vn, k, d)
        unless v
          return send_data("SERVER_ERROR data other than alist's format already exist.\r\n")
        end

# @logger.debug("#{s[2]} et=#{et}")
        v = expired_swap(v, data, et)

        v[0].push(data)
        v[1].push(Time.now.to_i)

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_expired_swap_and_push)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_expired_swap_and_push], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_expired_swap_and_sized_push <key> <expire-time> <array-size> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # the data expire-time's ago will be deleated.
      # the unit of the expire-time's is a second.
      # however,as follows when there is a suffix.
      # 'h' as +expire-time+ suffix is hour.
      # 'd' as +expire-time+ suffix is day.
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_expired_swap_and_sized_push(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[4].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        et = expired_str_to_i(s[2])
        return send_data("SERVER_ERROR format error in expire-time.\r\n") unless et

        v = to_alist_value_for_write(hname, vn, k, d)
        unless v
          return send_data("SERVER_ERROR data other than alist's format already exist.\r\n")
        end

# @logger.debug("#{s[2]} et=#{et}")
        v = expired_swap(v, data, et)

        max = s[3].to_i
        return send_data("NOT_PUSHED\r\n") if v[0].length >= max

        v[0].push(data)
        v[0] = v[0][0..(max - 1)]
        v[1].push(Time.now.to_i)
        v[1] = v[1][0..(max - 1)]

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_expired_swap_and_sized_push)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_expired_swap_and_sized_push], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_update_at <key> <index> <bytes>[forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def ev_alist_update_at(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        data = read_bytes(s[3].to_i)
        read_bytes(2)
        return forward2(nodes[0], s, data) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        return send_data("NOT_FOUND\r\n") unless ddata

        v = Marshal.load(ddata)

        idx = s[2].to_i
        return send_data("NOT_FOUND\r\n") if idx < 0 || v[0].length <= idx
        v[0][idx] = data
        v[1][idx] = Time.now.to_i

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          if @stats.wb_command_map.key?(:alist_update_at)
            Roma::WriteBehindProcess::push(hname, @stats.wb_command_map[:alist_update_at], k, data)
          end
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_shift <key> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END
      # |NOT_STORED|SERVER_ERROR <error message>)\r\n
      def ev_alist_shift(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, d)
        if ddata
          v = Marshal.load(ddata)
          return send_data("END\r\n") if v[0].length ==0
        else
          return send_data("END\r\n")
        end

        retv = v[0].shift
        v[1].shift
        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.read_count += 1
        @stats.write_count += 1

        if ret
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("VALUE #{s[1]} 0 #{retv.length}\r\n#{retv}\r\nEND\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end

      # alist_to_s <key> [index|range] [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <length of length string>\r\n
      # <length string>\r\n
      # VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def ev_alist_to_s(s)
        hname, k, d, vn, nodes = calc_hash(s[1])
        return forward1(nodes[0], s) if nodes[0] != @nid

        ddata = @storages[hname].get(vn, k, 0)
        @stats.read_count += 1

        return send_data("END\r\n") unless ddata
        v = to_alist_value(ddata)
        if v
          ret = nil
          if /(?:^(\d+)$|^(\d+)..((?:-)?\d+)$)/ =~ s[2]
            if $1
              ret = v[0][Range.new($1.to_i,$1.to_i)].to_s
            else
              ret = v[0][Range.new($2.to_i,$3.to_i)].to_s
            end
          else
            ret = v[0].to_s
          end
          len = v[0].length
          send_data("VALUE #{s[1]} 0 #{len.to_s.length}\r\n#{len.to_s}\r\n")
          return send_data("VALUE #{s[1]} 0 #{ret.length}\r\n#{ret}\r\nEND\r\n")
        else
          return send_data("SERVER_ERROR data other than alist's format already exist.\r\n")
        end
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @logger.error("#{e} #{$@}")
      end


      # alist_spushv <hash-name> <vnode-id>
      # src                                   dst
      #  |  ['alist_spushv' <hname> <vn>\r\n]->|
      #  |<-['READY'\r\n]                      |
      #  |                       [<dumpdata>]->|
      #  |                             :       |
      #  |                             :       |
      #  |                    [<end of dump>]->|
      #  |<-['STORED'\r\n]                     |
      def ev_alist_spushv(s)
        if s.length != 3
          @logger.error("#{__method__}:wrong number of arguments(#{s})")
          return send_data("CLIENT_ERROR Wrong number of arguments.\r\n")
        end
        if @stats.spushv_protection
          @logger.info("#{__method__}:In spushv_protection")
          return send_data("SERVER_ERROR In spushv_protection.\r\n")
        end
        @stats.run_receive_a_vnode["#{s[1]}_#{s[2]}"] = true

        $roma.stop_clean_up

        send_data("READY\r\n")

        count = 0
        loop {
          context_bin = read_bytes(20, 100)
          vn, last, clk, expt, klen = context_bin.unpack('NNNNN')
          break if klen == 0 # end of dump ?
          k = read_bytes(klen)
          vlen_bin = read_bytes(4, 100)
          vlen, =  vlen_bin.unpack('N')
          v = read_bytes(vlen, 100)
          val = to_alist_value(v)
          if val
# @logger.debug("listdata #{vn} #{k} #{val.inspect}")
             count += 1 if merge_list(s[1], vn, last, clk, expt, k, v, val)
          else
# @logger.debug("not listdata #{vn} #{k} #{val}")
            count += 1 if @storages[s[1]].load_stream_dump(vn, last, clk, expt, k, v)
          end
        }
        if @stats.spushv_protection
          @logger.info("#{__method__}:Canceled because of spushv_protection")
          send_data("CANCELED\r\n")
        else
          send_data("STORED\r\n")
        end
        @logger.debug("alist #{count} keys loaded.")
      rescue Storage::StorageException => e
        @logger.error("#{e.inspect} #{$@}")
        close_connection
        if Config.const_defined?(:STORAGE_EXCEPTION_ACTION) &&
            Config::STORAGE_EXCEPTION_ACTION == :shutdown
          @logger.error("#{__method__}:Romad will stop")
          @stop_event_loop = true
        end
      rescue => e
        @logger.error("#{e}\n#{$@}")
      ensure
        @stats.run_receive_a_vnode.delete("#{s[1]}_#{s[2]}") if s.length == 3
      end

      private

      def expired_swap(v, rcv_val, et)
        del = [rcv_val]
        expt =  Time.now.to_i - et
        v[1].each_with_index{|t,i|
# @logger.debug("v=#{v[0][i]} expt=#{expt} t=#{t} #{expt >= t}")
          del << v[0][i] if expt >= t
        }
        del.each{|dat|
          i = v[0].index(dat)
          if i
            v[0].delete_at(i)
            v[1].delete_at(i)
          end
        }
        v
      end

      def expired_str_to_i(s)
        if s.upcase =~ /(\d+)([H|D])?/
          t = $1.to_i
          if $2 == 'D'
            t *= 86400
          elsif $2 == 'H'
            t *= 3600
          end
          t
        else
          nil
        end
      end

      def to_alist_value_for_write(hname, vn, k, d)
        ddata = @storages[hname].get(vn, k, d)
        unless ddata
          v = [[],[]]
        else
          v = to_alist_value(ddata)
        end
      end

      def to_alist_value(v)
        # Marshal.dump([[],[]])[0..3].unpack("cc a c")
        # => [4, 8, "[", 7]
        # marshal format version 4.8
        # array object "["
        # array.length fixednum format 7 (7-5=2)
        return nil if v == nil || v[0..3] != "\x04\b[\a"
        val = Marshal.load(v)
        if val[0].instance_of?(Array) && val[1].instance_of?(Array)
          return val
        else
          return nil
        end
      rescue
        nil
      end

      def merge_list(hname, vn, last, clk, expt, k, v, val)
        ddata = @storages[hname].get(vn, k, 0)
        if ddata
          lv = Marshal.load(ddata)
          lv[0].each{|buf|
            idx = val[0].index(buf)
            if idx
              val[0].delete_at(idx)
              val[1].delete_at(idx)
            end
          }
          lv[0] += val[0]
          lv[1] += val[1]
          @storages[hname].set(vn, k, 0, expt ,Marshal.dump(lv))
        else
          @storages[hname].load_stream_dump(vn, last, clk, expt, k, v)
        end
      end

      def calc_hash(key)
        k,hname = key.split("\e")
        hname ||= @defhash
        d = Digest::SHA1.hexdigest(k).hex % @rttable.hbits
        vn = @rttable.get_vnode_id(d)
        nodes = @rttable.search_nodes_for_write(vn)
        [hname, k, d, vn, nodes]
      end

      # for a several lines received command
      def forward1(nid, rs, data=nil)
        if rs.last == "forward"
          return send_data("SERVER_ERROR Routing table is inconsistent.\r\n")
        end

        @logger.warn("forward #{rs} to #{nid}");

        buf = ''
        rs.each{|ss| buf << "#{ss} " }
        buf << "forward\r\n"
        if data
          buf << data
          buf << "\r\n"
        end

        con = get_connection(nid)
        con.send(buf)

        buf = con.gets
        if buf == nil
          @rttable.proc_failed(nid)
          @logger.error("forward get failed:nid=#{nid} rs=#{rs} #{$@}")
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        elsif buf.start_with?("ERROR")
          @rttable.proc_succeed(nid)
          con.close_connection
          @logger.error("forward get failed:nid=#{nid} rs=#{rs} #{$@}")
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        elsif buf.start_with?("VALUE") == false
          return_connection(nid, con)
          @rttable.proc_succeed(nid)
          return send_data(buf)
        end

        res = ''
        begin
          res << buf
          s = buf.split(/ /)
          if s[0] != 'VALUE'
            return_connection(nid, con)
            @rttable.proc_succeed(nid)
            return send_data(buf)
          end
          res << con.read_bytes(s[3].to_i + 2)
        end while (buf = con.gets)!="END\r\n"

        res << "END\r\n"

        return_connection(nid, con)
        @rttable.proc_succeed(nid)

        send_data(res)
      rescue => e
        @rttable.proc_failed(nid) if e.message != "no connection"
        @logger.error("forward get failed:nid=#{nid} rs=#{rs} #{e} #{$@}")
        send_data("SERVER_ERROR Message forward failed.\r\n")
      end

      # for a one line reveived command
      def forward2(nid, rs, data=nil)
        if rs.last == "forward"
          return send_data("SERVER_ERROR Routing table is inconsistent.\r\n")
        end

        @logger.warn("forward #{rs} to #{nid}");

        buf = ''
        rs.each{|ss| buf << "#{ss} " }
        buf << "forward\r\n"
        if data
          buf << data
          buf << "\r\n"
        end

        res = send_cmd(nid, buf)
        if res == nil || res.start_with?("ERROR")
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        end
        send_data("#{res}\r\n")
      end

    end #  PluginAshiatoList
  end # CommandPlugin
end # Roma
