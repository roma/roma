#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
require 'optparse'
require 'roma/routing/routing_data'

# ダイジェストのビット数
dgst_bits=32
# 分割ビット数 (dgst_bits >= div_bits)
div_bits=9
# 冗長度 (nodes.length >= rn)
rn=2
# ホスト名の重複許可
repeathost=false

opts = OptionParser.new
opts.banner = "usage:#{File.basename($0)} [options] node-id..."
opts.on("-h","--hash [bits]","(default=32)"){|v| dgst_bits = v.to_i }
opts.on("-d","--divide [bits]","(default=9)"){|v| div_bits = v.to_i }
opts.on("-r","--redundant [num]","(default=2)"){|v| rn = v.to_i }
opts.on(nil,"--enabled_repeathost"){|v| repeathost=true }
opts.parse!(ARGV)

nodes = ARGV
nodes.map!{|n| n.sub(':','_')}

if nodes.length == 0
  STDERR.puts opts.help
  exit!
end

if dgst_bits < div_bits
  STDERR.puts "The hash bits should be divide bits or more."
  exit!  
end

if div_bits > 32
  STDERR.puts "The upper bound of divide bits is 32."
  exit!    
end

if nodes.length < rn
  STDERR.puts "The node-id number should be redundant number or more."
  exit!
end

rt = Roma::Routing::RoutingData::create(dgst_bits,div_bits,rn,nodes,repeathost)

nodes.each{|nid|
  rt.save("#{nid}.route")
}
puts "nodes => #{nodes}"
puts "Routing table has created."
