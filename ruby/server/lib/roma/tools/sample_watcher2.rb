#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
require 'roma/logging/rlogger'
require 'roma/messaging/con_pool'
require 'roma/routing/routing_data'
require 'roma/client/sender'

module Roma
  
  class Watcher

    def initialize
      @sender = Roma::Client::Sender.new
    end

    def watch(nid, command)
      ret = @sender.send_command(nid, command, value = nil, :multiplelines_receiver)
    end
  end # class Watcher
  
end # module Roma

w = Roma::Watcher.new

# the definition of all nodes 
nodes = ['roma0:11211','roma0:11212']

nodes.each{|nid|
  begin
    ret = w.watch(nid, "stat #{ARGV[0]}")
    puts "/********** #{nid} **********/"
    ret.each{|l|
      puts l
    }
  rescue
    STDERR.puts "command error in #{nid}."
  end
}

puts "#{Time.now} ROMA watcher2 has done."
