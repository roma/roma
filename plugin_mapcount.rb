require 'roma/command/command_definition'

module Roma
  module CommandPlugin

    module PluginMapCount
      include ::Roma::CommandPlugin
      include ::Roma::Command::Definition

      # mapcount_countup <key> <expt> <sub_keys_length>\r\n
      # <sub_keys> \r\n
      #
      # (
      # VALUE <key> 0 <length of json string>\r\n
      # <json string>\r\n
      # END\r\n
      # |CLIENT_ERROR invalid sub_keys format: <sub_keys>\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_write_command_with_key_value :mapcount_countup, 3, :multi_line do |ctx|
        v = {}
        v = marshal_load(ctx.stored.value) if ctx.stored

        args = ctx.params.value.split(/\s*,\s*/)
        args.each do |arg|
          if arg =~ /^([A-Za-z0-9]+)(:(\-?[\d]+))?$/
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
      # (
      # [VALUE <key> 0 <length of json string>\r\n
      # <json string>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_write_command_with_key_value :mapcount_update, 3, :multi_line do |ctx|
        next send_data("END\r\n") unless ctx.stored

        v = {}
        v = marshal_load(ctx.stored.value)

        if v.is_a?(Hash)
          args = ctx.params.value.split(/\s*,\s*/)
          if args.count == 0
            ret = return_str(v)
          else
            ret = {}
            ret[:last_updated_date] = v[:last_updated_date]
            args.each do |arg|
              ret[arg] = v[arg] if v[arg] != nil
            end
            ret = return_str(ret)
          end
        end

        v[:last_updated_date] = Time.now.gmtime.strftime(DATE_FORMAT)
        expt = chg_time_expt(ctx.argv[2].to_i)

        ret_msg = "VALUE #{ctx.params.key} 0 #{ret.length}\r\n#{ret}\r\nEND"
        [0, expt, Marshal.dump(v), :write, ret_msg]
      end

      # mapcount_get <key> 0 <sub_keys_str_len>\r\n
      # <sub_keys>\r\n
      #
      # (
      # [VALUE <key> 0 <length of json string>\r\n
      # <json string>\r\n]
      # END\r\n
      # |SERVER_ERROR <error message>\r\n)
      def_read_command_with_key_value :mapcount_get, 3, :multi_line do |ctx|
        ret = nil
        if ctx.stored
          ret_val = marshal_load(ctx.stored.value)

          if ret_val.is_a?(Hash)
            args = ctx.params.value.split(/\s*,\s*/)
            if args.count == 0
              ret = return_str(ret_val)
            else
              ret = {}
              ret[:last_updated_date] = ret_val[:last_updated_date]
              args.each do |arg|
                ret[arg] = ret_val[arg] if ret_val[arg] != nil
              end
              ret = return_str(ret)
            end
          end
        end

        send_data("VALUE #{ctx.params.key} 0 #{ret.length}\r\n#{ret}\r\n") if ret
        send_data("END\r\n")
      end

      private
      def return_str(data)
        data.to_json
      end

      def marshal_load(data)
        begin
          Marshal.load(data)
        rescue => e
          msg = "SERVER_ERROR #{e} #{$@}".tr("\r\n"," ")
          send_data("#{msg}\r\n")
          @log.error("#{e} #{$@}")
        end
      end

      DATE_FORMAT = "%Y-%m-%dT%H:%M:%S +00"
    end # PluginMapCount
  end # CommandPlugin
end # Roma
