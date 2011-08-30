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

    def send_cmd(cmd, nid = nil)
      nid = @rd.nodes[0] unless nid
      res = ''
      res << @sender.send_command(nid, cmd, nil, :multiplelines_receiver2).join("\r\n")
      res << "\r\n"
    end

    def send_cmd_all(cmd)
      res = ''
      @rd.nodes.each{|nid|
        res << "****** #{nid}\r\n"
        res << send_cmd(cmd, nid)
      }
      res
    end

  end # class MultiCommander
  
end # module Roma
