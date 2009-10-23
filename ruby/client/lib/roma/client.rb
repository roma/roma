require 'roma/client/rclient'

# = Overview
# The ROMA client library is used for roma client.
#
# == Example
# require 'rubygems'
# require 'roma/client'
#
# nodes = ['host1:11211', 'host2:11211']
# client = Roma::Client::RomaClient.new(nodes)
#
# key = 'key'
# res = client.set(key, 'valie')
# puts "put:#{res}"
#
# puts "get:#{client.get key}"
#
# res = client.delete key
# puts "del:#{res}"
# puts "get:#{client.get key}"
#
