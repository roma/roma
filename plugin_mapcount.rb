require 'roma/command/command_definition'

module Roma
  module CommandPlugin

    module PluginCount
      include ::Roma::CommandPlugin
      include ::Roma::Command::Definition

      def_write_command_with_key_value :mapcount_set, 5 do |ctx|
      #def_write_command_with_key :mapcount_set do |ctx|
        v = {}
        v = Marshal.load(ctx.stored.value) if ctx.stored

        args = ctx.argv[2].split(/\s*,\s*/)
        args.each do |arg|
=begin
          if v.key? arg
            v[arg] += 1
          else
            v[arg] = 0
          end
=end
            v[arg] = ctx.params.value
            #v[arg] = "111"
        end

        expt = ctx.argv[4].to_i
        if expt == 0
          expt = 0x7fffffff
        elsif expt < 2592000
          expt += Time.now.to_i
        end

        #[0, expt, Marshal.dump(v), :write, 'STORED']
        [0, ctx.stored.expt, Marshal.dump(v), :write, 'STORED']
      end

    end # PluginCount
  end # CommandPlugin
end # Roma
