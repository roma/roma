require 'roma/command/command_definition'

module Roma
  module CommandPlugin

    module PluginCount
      include ::Roma::CommandPlugin
      include ::Roma::Command::Definition

      # mapcount_countup <key> <expt> <sub_keys_length>\r\n
      # <sub_keys> \r\n
      #
      # TODO: comment for return value
      def_write_command_with_key_value :mapcount_countup, 3, :multi_line do |ctx|
        v = {}
        v = Marshal.load(ctx.stored.value) if ctx.stored

        args = ctx.params.value.split(/\s*,\s*/)
        args.each do |arg|
          if arg =~ /^([A-Za-z0-9]+)(:(\-?[\d]))?$/
            key = $1
            count = 1
            count = $3.to_i if $3
            v[key] ||= 0
            v[key] += count
          else
            raise ClientErrorException, "invalid sub_keys format: #{ctx.params.value}"
          end
        end

        v[:last_updated_date] = Time.now.gmtime.strftime(DATE_FORMAT)
        expt = chg_time_expt(ctx.argv[2].to_i)

        ret_str = return_str(v)
        ret_msg = "VALUE #{ctx.params.key} 0 #{ret_str.length}\r\n#{ret_str}\r\nEND"
        [0, expt, Marshal.dump(v), :write, ret_msg]
      end

      # mapcount_update <key> <expt> <sub_keys_length>\r\n
      # <sub_keys>\r\n
      #
      # TODO: handle sub_keys
      # TODO: comment for return value
      def_write_command_with_key :mapcount_update, :multi_line do |ctx|
        v = {}
        v = Marshal.load(ctx.stored.value) if ctx.stored

        v[:last_updated_date] = Time.now.gmtime.strftime(DATE_FORMAT)
        expt = chg_time_expt(ctx.argv[2].to_i)

        ret_str = return_str(v)
        ret_msg = "VALUE #{ctx.params.key} 0 #{ret_str.length}\r\n#{ret_str}\r\nEND"
        [0, expt, Marshal.dump(v), :write, ret_msg]
      end

      # mapcount_get <key> 0 <sub_keys_str_len>\r\n
      # <sub_keys>\r\n
      #
      # TODO: handle sub_keys
      # TODO: comment for return value
      def_read_command_with_key :mapcount_get, :multi_line do |ctx|
        ret = nil
        if ctx.stored
          ret_val = Marshal.load(ctx.stored.value)
          if ret_val.is_a?(Hash)
            ret = return_str(ret_val)
          end
        end

        send_data("VALUE #{ctx.params.key} 0 #{ret.length}\r\n#{ret}\r\n") if ret
        send_data("END\r\n")
      end

      private
      def return_str(data)
        data.to_json
      end

      DATE_FORMAT = "%Y-%m-%dT%H:%M:%S +00"
    end # PluginCount
  end # CommandPlugin
end # Roma
