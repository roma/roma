#!/usr/bin/env ruby

#require 'socket'

module Roma
  class Adm
    def initialize(cmd)
      @cmd = cmd.dup #to avoid forzen error
    end

    def check_type
      case @cmd
      when "balse", "shutdown_self" # yes/no check
        puts("Are you sure?(yes/no)\r\n")
        if STDIN.gets.chomp != "yes"
          raise "confirmation was rejected"
        else
          @cmd.concat("\r\nyes\r\nyes\r\n")
        end
      when "start" # alias cmd
        puts("Please input hostname or ip address which is used for ROMA.\r\n")
        hostname = STDIN.gets.chomp
        puts("Please input port No. which is used for ROMA.\r\n")
        port = STDIN.gets.chomp
        puts("Please input PATH of config.rb.\r\n")
        config_path = STDIN.gets.chomp
        @cmd = "bin/romad #{hostname} -p #{port} -d --config #{config_path}"
        @alias = true
      else
    #  when "detach"
    #    @check = true
    #  when "start", "recover-node", "dump-ring"
    #    @alias = true
    #  else
        @alias = false # no need?
    #    @check = false # no need?
      end
    end

    def make_command
      #case @cmd
      #when "detach"
      #  @cmd = "shutdown-self\r\nyes\r\n"
      #when "start"
      #  
      #when "join"
      #when "snapshot"
      #when "status"
      #else
        
      #end
    end

    def send_command(node="localhost_10001")
      if @alias
        base_path = Pathname(__FILE__).dirname.parent.parent.parent.expand_path
        `#{base_path}/#{@cmd}`
      #elsif @check
      else
        puts `echo -e "#{@cmd}" | nc -i1 #{node.split("_")[0]} #{node.split("_")[1]}`
      end
    end
  end #Adm
end # Roma

