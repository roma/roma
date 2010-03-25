#!/usr/bin/env ruby
#
# usage:chg_redundancy n address port > routingfile
#
require 'roma/commons'
require 'roma/client/sender'

def get_routing_data(nid)
  sender = Roma::Client::Sender.new
  sender.send_routedump_command(nid)
end

if ARGV.length != 3
  STDERR.puts "usage:#{File.basename($0)} n address port";exit
end

rn = ARGV[0].to_i
STDERR.puts "must be n >= 1.";exit if rn < 1

rd = get_routing_data("#{ARGV[1]}_#{ARGV[2]}")
STDERR.puts "can not get the routing data.";exit unless rd

# clear logic clock
rd.v_clk.keys.each{|k| rd.v_clk[k] = 0 }

if rd.rn > rn
  rd.v_idx.keys.each{|k|
    rd.v_idx[k] = rd.v_idx[k][0..(rn - 1)]
  }
end

rd.rn = rn
rd.nodes.sort!
STDOUT.puts YAML.dump(rd)
