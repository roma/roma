require 'roma/messaging/con_pool'
require 'roma/command/command_definition'

module Roma
  module CommandPlugin

    module PluginMap
      include Roma::CommandPlugin
      include Roma::Command::Definition
      
      # map_set <key> <mapkey> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def_command_with_key_value :map_set, 3 do |s, k, hname, d, vn, nodes, data|
        v = {}
        ddata = @storages[hname].get(vn, k, d)
        v = Marshal.load(ddata) if ddata
          
        v[s[2]] = data
        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt , Marshal.dump(v))
        @stats.write_count += 1
          
        if ret
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("STORED\r\n")
        else
          send_data("NOT_STORED\r\n")
        end
      end

      # map_get <key> <mapkey> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_command_with_key :map_get, :multi_line do |s, k, hname, d, vn, nodes|
        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1
        if ddata
          v = Marshal.load(ddata)[s[2]]
          send_data("VALUE #{s[1]} 0 #{v.length}\r\n#{v}\r\n") if v
        end
        send_data("END\r\n")
      end

      # map_delete <key> <mapkey> [forward]\r\n
      #
      # (DELETED|NOT_DELETED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_command_with_key :map_delete do |s, k, hname, d, vn, nodes|
        ddata = @storages[hname].get(vn, k, d)
        next send_data("NOT_FOUND\r\n") unless ddata

        v = Marshal.load(ddata)
        next send_data("NOT_DELETED\r\n") unless v.key?(s[2])
        
        v.delete(s[2])
        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump(v))
        @stats.write_count += 1

        if ret
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("DELETED\r\n")
        else
          send_data("NOT_DELETED\r\n")
        end
      end

      # map_clear <key> [forward]\r\n
      #
      # (CLEARED|NOT_CLEARED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_command_with_key :map_clear do |s, k, hname, d, vn, nodes|
        ddata = @storages[hname].get(vn, k, d)
        next send_data("NOT_FOUND\r\n") unless ddata

        expt = 0x7fffffff
        ret = @storages[hname].set(vn, k, d, expt ,Marshal.dump({}))
        @stats.delete_count += 1

        if ret
          redundant(nodes[1..-1], hname, k, d, ret[2], expt, ret[4])
          send_data("CLEARED\r\n")
        else
          send_data("NOT_CLEARED\r\n")
        end          
      end

      # map_size <key> [forward]\r\n
      #
      # (<length>|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_command_with_key :map_size do |s, k, hname, d, vn, nodes|
        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        next send_data("NOT_FOUND\r\n") unless ddata
        ret = Marshal.load(ddata).size
        send_data("#{ret}\r\n")
      end

      # map_key? <key> <mapkey> [forward]\r\n
      #
      # (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_command_with_key :map_key? do |s, k, hname, d, vn, nodes|
        map_exists? s, k, hname, d, vn, nodes, :key?
      end
      
      # map_value? <key> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_command_with_key :map_value? do |s, k, hname, d, vn, nodes|
        map_exists? s, k, hname, d, vn, nodes, :value?
      end
      
      def map_exists?(s, k, hname, d, vn, nodes, method)
        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        return send_data("NOT_FOUND\r\n") unless ddata
        ret = Marshal.load(ddata).send method, s[2]
        send_data("#{ret}\r\n")
      rescue => e
        send_data("SERVER_ERROR #{e} #{$@}\r\n")
        @log.error("#{e} #{$@}") 
      end
      private :map_exists?

      # map_empty? <key> [forward]\r\n
      #
      # (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_command_with_key :map_empty? do |s, k, hname, d, vn, nodes|
        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1

        next send_data("NOT_FOUND\r\n") unless ddata
        v = Marshal.load(ddata)
        send_data("#{v.empty?}\r\n")
      end

      # map_keys <key> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <length of length string>\r\n
      # <length string>\r\n
      # (VALUE <key> 0 <value length>\r\n
      # <value>\r\n)*
      # ]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_command_with_key :map_keys, :multi_line do |s, k, hname, d, vn, nodes|
        map_return_array s, k, hname, d, vn, nodes, :keys
      end

      # map_values <key> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <length of length string>\r\n
      # <length string>\r\n
      # (VALUE <key> 0 <value length>\r\n
      # <value>\r\n)*
      # ]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_command_with_key :map_values, :multi_line do |s, k, hname, d, vn, nodes|
        map_return_array s, k, hname, d, vn, nodes, :values
      end

      def map_return_array(s, k, hname, d, vn, nodes, method)
        ddata = @storages[hname].get(vn, k, 0)
        @stats.read_count += 1

        if ddata
          v = Marshal.load(ddata).send method
          len = v.length
          send_data("VALUE #{s[1]} 0 #{len.to_s.length}\r\n#{len.to_s}\r\n")
          v.each{|val|
            send_data("VALUE #{s[1]} 0 #{val.length}\r\n#{val}\r\n")
          }
        end
        send_data("END\r\n")
      rescue => e
        msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
        send_data("#{msg}\r\n")
        @log.error("#{e} #{$@}")
      end
      private :map_return_array

      # map_to_s <key> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_command_with_key :map_to_s, :multi_line do |s, k, hname, d, vn, nodes|
        ddata = @storages[hname].get(vn, k, d)
        @stats.read_count += 1
        if ddata
          v = Marshal.load(ddata).inspect
          send_data("VALUE #{s[1]} 0 #{v.length}\r\n#{v}\r\n")
        end
        send_data("END\r\n")
      end

    end # module PluginMap
  end # module CommandPlugin
  
  
  module ClientPlugin
    
    module PluginMap
      
      def map_set(key, mapkey, value)
        value_validator(value)
        sender(:oneline_receiver, key, value, "map_set %s #{mapkey} #{value.length}")
      end

      def map_get(key, mapkey)
        ret = sender(:value_list_receiver, key, nil, "map_get %s #{mapkey}")
        return nil if ret==nil || ret.length == 0
        ret[0]
      end

      def map_delete(key, mapkey)
        sender(:oneline_receiver, key, nil, "map_delete %s #{mapkey}")
      end

      def map_clear(key)
        sender(:oneline_receiver, key, nil, "map_clear %s")
      end

      def map_size(key)
        ret = sender(:oneline_receiver, key, nil, "map_size %s")
        return ret.to_i if ret =~ /\d+/
        ret
      end

      def map_key?(key, mapkey)
        ret = sender(:oneline_receiver, key, nil, "map_key? %s #{mapkey}")
        if ret == 'true'
          true
        elsif ret == 'false'
          false
        else
          ret
        end
      end

      def map_value?(key, value)
        ret = sender(:oneline_receiver, key, nil, "map_value? %s #{value}")
        if ret == 'true'
          true
        elsif ret == 'false'
          false
        else
          ret
        end        
      end

      def map_empty?(key)
        ret = sender(:oneline_receiver, key, nil, "map_empty? %s")
        if ret == 'true'
          true
        elsif ret == 'false'
          false
        else
          ret
        end
      end

      def map_keys(key)
        ret = sender(:value_list_receiver, key, nil, "map_keys %s")
        return nil if ret.length == 0
        ret[0] = ret[0].to_i
        ret
      end

      def map_values(key)
        ret = sender(:value_list_receiver, key, nil, "map_values %s")
        return nil if ret.length == 0
        ret[0] = ret[0].to_i
        ret
      end

      def map_to_s(key)
        ret = sender(:value_list_receiver, key, nil, "map_to_s %s")
        return nil if ret.length == 0
        ret[0]
      end

      private

      def value_validator(value)
        if value == nil || !value.instance_of?(String)
          raise "value must be a String object."
        end
      end

    end # module PluginMap
  end # module ClientPlugin

end # module Roma
 

