#!/usr/bin/env ruby
#
# usage:ssroute address_port
#
require 'roma/routing/routing_data'

if ARGV.length!=1
  puts "usage:ssroute address:port"
  exit
end

ap = ARGV[0].sub(':','_')

begin
  if Roma::Routing::RoutingData::snapshot("#{ap}.route")
    puts "succeed"
  else
    puts "Routing-log file does not found."
  end
rescue =>e
  puts "error:#{e}"
end
