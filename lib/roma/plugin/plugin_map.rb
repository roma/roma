require 'roma/messaging/con_pool'
require 'roma/command/command_definition'

module Roma
  module CommandPlugin

    module PluginMap
      include Roma::CommandPlugin
      include Roma::Command::Definition
      
      # map_set <key> <mapkey> <flags> <expt> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
      def_write_command_with_key_value :map_set, 5 do |ctx|
        v = {}
        v = Marshal.load(ctx.stored.value) if ctx.stored
          
        v[ctx.argv[2]] = ctx.params.value
        expt = chg_time_expt(ctx.argv[4].to_i)
        
        # [flags, expire time, value, kind of counter(:write/:delete), result message]
        [0, expt, Marshal.dump(v), :write, 'STORED']
      end

      # map_get <key> <mapkey> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_read_command_with_key :map_get, :multi_line do |ctx|
        if ctx.stored
          v = Marshal.load(ctx.stored.value)[ctx.argv[2]]
          send_data("VALUE #{ctx.params.key} 0 #{v.length}\r\n#{v}\r\n") if v
        end
        send_data("END\r\n")
      end

      # map_delete <key> <mapkey> [forward]\r\n
      #
      # (DELETED|NOT_DELETED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_write_command_with_key :map_delete do |ctx|
        next send_data("NOT_FOUND\r\n") unless ctx.stored

        v = Marshal.load(ctx.stored.value)
        next send_data("NOT_DELETED\r\n") unless v.key?(ctx.argv[2])
        
        v.delete(ctx.argv[2])
        
        [0, ctx.stored.expt, Marshal.dump(v), :delete, 'DELETED']
      end

      # map_clear <key> [forward]\r\n
      #
      # (CLEARED|NOT_CLEARED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_write_command_with_key :map_clear do |ctx|
        next send_data("NOT_FOUND\r\n") unless ctx.stored

        [0, ctx.stored.expt, Marshal.dump({}), :delete, 'CLEARED']
      end


      # map_size <key> [forward]\r\n
      #
      # (<length>|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_read_command_with_key :map_size do |ctx|
        if ctx.stored
          ret = Marshal.load(ctx.stored.value).size
          send_data("#{ret}\r\n")
        else
          send_data("NOT_FOUND\r\n")
        end
      end

      # map_key? <key> <mapkey> [forward]\r\n
      #
      # (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_read_command_with_key :map_key? do |ctx|
        if ctx.stored
          ret = Marshal.load(ctx.stored.value).key? ctx.argv[2]
          send_data("#{ret}\r\n")
        else
          send_data("NOT_FOUND\r\n")
        end
      end
      
      # map_value? <key> <bytes> [forward]\r\n
      # <data block>\r\n
      #
      # (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_read_command_with_key_value :map_value?, 2 do |ctx|
        if ctx.stored
          ret = Marshal.load(ctx.stored.value).value? ctx.params.value
          send_data("#{ret}\r\n")
        else
          send_data("NOT_FOUND\r\n")
        end
      end
      
      # map_empty? <key> [forward]\r\n
      #
      # (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
      def_read_command_with_key :map_empty? do |ctx|
        if ctx.stored
          v = Marshal.load(ctx.stored.value)
          send_data("#{v.empty?}\r\n")
        else
          send_data("NOT_FOUND\r\n")
        end
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
      def_read_command_with_key :map_keys, :multi_line do |ctx|
        if ctx.stored
          v = Marshal.load(ctx.stored.value).keys
          len = v.length
          send_data("VALUE #{ctx.params.key} 0 #{len.to_s.length}\r\n#{len.to_s}\r\n")
          v.each{|val|
            send_data("VALUE #{ctx.params.key} 0 #{val.length}\r\n#{val}\r\n")
          }
        end
        send_data("END\r\n")
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
      def_read_command_with_key :map_values, :multi_line do |ctx|
        if ctx.stored
          v = Marshal.load(ctx.stored.value).values
          len = v.length
          send_data("VALUE #{ctx.params.key} 0 #{len.to_s.length}\r\n#{len.to_s}\r\n")
          v.each{|val|
            send_data("VALUE #{ctx.params.key} 0 #{val.length}\r\n#{val}\r\n")
          }
        end
        send_data("END\r\n")
      end

      # map_to_s <key> [forward]\r\n
      #
      # (
      # [VALUE <key> 0 <value length>\r\n
      # <value>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_read_command_with_key :map_to_s, :multi_line do |ctx|
        if ctx.stored
          v = Marshal.load(ctx.stored.value).inspect
          send_data("VALUE #{ctx.params.key} 0 #{v.length}\r\n#{v}\r\n")
        end
        send_data("END\r\n")
      end

    end # module PluginMap
  end # module CommandPlugin
end # module Roma
 

