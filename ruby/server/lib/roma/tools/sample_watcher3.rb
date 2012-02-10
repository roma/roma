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
    def get_node_list(nid)
      @sender.send_command(nid, "nodelist").split(' ')
    end
  end # class Watcher
end # module Roma

nodes = [
'localhost:11211',
'localhost:11212',
'localhost:11213'
]

puts "#{Time.now} ROMA watcher has started."
puts ""

all_ring = []

nodes.each{ |nid|
  puts "=> check a process #{nid} with a nodelist command"
  begin
    w = Roma::Watcher.new
    ring = w.get_node_list(nid)
    all_ring << ring unless all_ring.include? ring
  rescue => e
    STDERR.puts "    command error in #{nid}: #{e.inspect}"
  end
}

puts ""

all_ring.each { |ring|
  puts "#{ring}"
  puts "size: #{ring.size}"
}

puts ""
puts "#{Time.now} ROMA watcher has done."
