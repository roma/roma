#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
require 'roma/commons'
require 'roma/client/sender'

module Roma
  
  class MultiCommander

    Roma::Client::Sender.class_eval{
      def multiplelines_receiver2(con) 
        ret = []
        while select [con], nil, nil, 0.05
          ret << con.gets.chomp
        end
        ret
      end
    }

    def initialize(nid)
      @sender = Roma::Client::Sender.new
      @rd = @sender.send_routedump_command(nid)
    end

    def send_cmd(cmd)
      res = ''
      @rd.nodes.each{|nid|
        res << "****** #{nid}\r\n"
        res << @sender.send_command(nid, cmd, nil, :multiplelines_receiver2).join("\r\n")
        res << "\r\n"
      }
      res
    end

  end # class MultiCommander
  
end # module Roma

if ARGV.length < 2
  STDERR.puts "usage:#{File.basename($0)} addr_port command args..."
  exit
end

w = Roma::MultiCommander.new(ARGV[0])

STDOUT.puts w.send_cmd(ARGV[1..-1].join(' '))
