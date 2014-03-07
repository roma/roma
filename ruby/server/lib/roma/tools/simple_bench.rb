#!/usr/bin/env ruby
require 'optparse'
require 'date'
require 'roma/client/rclient'

@cnt = 0
@tmax = 0
@tmin = 100

Thread.new do
  sleep_time=10
  while(true)
    sleep sleep_time
    printf("qps=%d max=%f min=%f ave=%f\n",@cnt/sleep_time,@tmax,@tmin,sleep_time/@cnt.to_f)
    @cnt=0
    @tmax=0
    @tmin=100
  end
end

def random_request_sender(ini_nodes, n)
  puts __method__
  rc=Roma::Client::RomaClient.new(ini_nodes)

  loop do
    i=rand(n)
    ts = DateTime.now
    case rand(3)
    when 0
      res=rc.set(i.to_s,'hoge'+i.to_s)
      puts "set k=#{i} #{res}" if res==nil || res.chomp != 'STORED'
    when 1
      res=rc.get(i.to_s)
      puts "get k=#{i} #{res}" if res == :error
    when 2
      res=rc.delete(i.to_s)
      puts "del k=#{i} #{res}" if res != 'DELETED' && res != 'NOT_FOUND'
    end
    t=(DateTime.now - ts).to_f * 86400.0
    @tmax=t if t > @tmax
    @tmin=t if t < @tmin
    @cnt+=1
  end
end
 
def set_sequence(ini_nodes, n)
  puts __method__
  rc=Roma::Client::RomaClient.new(ini_nodes)

  n.times do |i|
    ts = DateTime.now
    res=rc.set("key_#{i}","value_#{i}")
    puts "set k=#{i} #{res}" if res==nil || res.chomp != 'STORED'
    t=(DateTime.now - ts).to_f * 86400.0
    @tmax=t if t > @tmax
    @tmin=t if t < @tmin
    @cnt+=1
  end
end

def get_sequence(ini_nodes, n)
  puts __method__
  rc=Roma::Client::RomaClient.new(ini_nodes)

  n.times do |i|
    ts = DateTime.now
    res=rc.get("key_#{i}")
    puts "get #{i} #{res}" if res != "value_#{i}"
    t=(DateTime.now - ts).to_f * 86400.0
    @tmax=t if t > @tmax
    @tmin=t if t < @tmin
    @cnt+=1
  end
end

param = { :num=>10000, :th=>1 }

opts = OptionParser.new
opts.banner = "usage:#{File.basename($0)} [options] addr:port"
opts.on("-s", "--set", "set request"){|v| param[:set] = v }
opts.on("-g", "--get", "get request"){|v| param[:get] = v }
opts.on("-r", "--rand", "random request"){|v| param[:rand] = v }
opts.on("-n", "--num [num]", "number of keys"){|v| param[:num] = v.to_i }
opts.on("-t", "--threads [num]", "number of threads"){|v| param[:th] = v.to_i }
opts.parse!(ARGV)

if ARGV.length == 0
  STDERR.puts opts.help
  exit
end

if param.key?(:get) == false && param.key?(:set) == false
  param[:rand] = true
end

t = []
param[:th].times do |i|
  puts "Start thread #{i}"
  t << Thread.new do
    get_sequence(ARGV, param[:num]) if param.key?(:get)
    set_sequence(ARGV, param[:num]) if param.key?(:set)
    random_request_sender(ARGV, param[:num]) if param.key?(:rand)
  end
end

t.each{|th| th.join }

puts "#{File.basename($0)} has done."
