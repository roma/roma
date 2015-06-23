#!/usr/bin/env ruby

module Roma
  class Adm
    def initialize(cmd, port)
      @cmd = cmd.dup #to avoid frozen error
      @port = port
    end

    def check_type
      case @cmd
      when "balse", "shutdown" # yes/no check
        make_command("boolean")
      when "start" # alias cmd
        make_command("booting")
        @alias = true
      when /^(set|add|replace|append|prepend|cas|alist_delete|alist_include\?|alist_insert|alist_sized_insert|alist_swap_and_insert|alist_swap_and_sized_insert|alist_join_with_time|alist_join|alist_push|alist_sized_push|alist_swap_and_push|alist_update_at)/ # waiting value input command
        make_command("value")
      else
        t = Thread.new do
          loop{
            print "."
            sleep 1
          }
        end
      end
    end

    def send_command(host="localhost")
      if @alias
        base_path = Pathname(__FILE__).dirname.parent.parent.parent.expand_path
        `#{base_path}/#{@cmd}`
      else
        return `echo -e "#{@cmd}" | nc -i1 #{host} #{@port}`
      end
    end

    private

    def make_command(type)
      case type
      when "boolean"
        puts("Are you sure?(yes/no)\r\n")
        if STDIN.gets.chomp != "yes"
          raise "confirmation was rejected"
        else
          @cmd.concat("\r\nyes\r\nyes\r\n")
        end
      when "booting"
        puts("Please input hostname or ip address which is used for ROMA.\r\n  Ex.) roma_serverA, 192.168.33.11\r\n")
        hostname = STDIN.gets.chomp
        puts("Please input PATH of config.rb.\r\n  Ex.) /home/roma/config.rb\r\n")
        config_path = STDIN.gets.chomp
        @cmd = "bin/romad #{hostname} -p #{@port} -d --config #{config_path}"
      when "value"
        puts("Please input value.\r\n")
        value = STDIN.gets.chomp
        @cmd.concat("\r\n#{value}\r\n")
      end
    end

  end #Adm
end # Roma

