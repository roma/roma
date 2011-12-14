require 'roma/command/command_definition'

module Roma
  module CommandPlugin

    module PluginCount
      include ::Roma::CommandPlugin
      include ::Roma::Command::Definition

      # mapcount_set <key> <flags> <expt> <sub_keys>\r\n
      # TODO: comment for return value
      def_write_command_with_key :mapcount_set do |ctx|
        v = {}
        v = Marshal.load(ctx.stored.value) if ctx.stored

        args = ctx.argv[4].split(/\s*,\s*/)
        args.each do |arg|
          if arg =~ /^([A-Za-z0-9]+)(:(\-?[\d]))?$/
            key = $1
            count = 1
            count = $3.to_i if $3
            v[key] ||= 0
            v[key] += count
          else
            raise ClientErrorException, "invalid sub_keys format: #{ctx.argv[4]}"
          end
        end

        expt = calc_expt(ctx.argv[3].to_i)

        [0, expt, Marshal.dump(v), :write, 'STORED']
      end

      private
      def calc_expt(expt)
        if expt == 0
          expt = 0x7fffffff
        elsif expt < 2592000
          expt += Time.now.to_i
        end

        expt
      end
    end # PluginCount
  end # CommandPlugin
end # Roma
