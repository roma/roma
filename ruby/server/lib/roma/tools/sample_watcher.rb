#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
require 'roma/commons'
require 'roma/client/sender'

module Roma
  
  class Watcher

    def initialize
      @sender = Roma::Client::Sender.new
    end

    def get_node_list(nid)
      @sender.send_command(nid, "nodelist").split(' ')
    end

  end # class Watcher
  
end # module Roma


w = Roma::Watcher.new

# 監視ノードの定義
nodes = ['roma0:11211','roma0:11212','roma0:11213','roma0:11214']

nodes.each{|nid|
  begin
    if w.get_node_list(nid).length != nodes.length
      STDERR.puts "fail over occurred in #{nid}."
    end
  rescue
    STDERR.puts "command error in #{nid}."
  end
}

puts "#{Time.now} ROMA watcher has done."
