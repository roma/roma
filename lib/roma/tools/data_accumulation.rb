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

def randstr(n)
  s = ("a".."z").to_a
  n.times.map{ s.sample }.join
end

def set_sequence(ini_nodes, n, v_size)
  puts __method__
  rc=Roma::Client::RomaClient.new(ini_nodes)

  n.times do |i|
    ts = DateTime.now
    res=rc.set("key_#{i}", randstr(v_size))
    puts "set k=#{i} #{res}" if res==nil || res.chomp != 'STORED'
    t=(DateTime.now - ts).to_f * 86400.0
    @tmax=t if t > @tmax
    @tmin=t if t < @tmin
    @cnt+=1
  end
end

# default
param = { :num=>1000, :value_size=>1024 }

opts = OptionParser.new
opts.banner = "usage:#{File.basename($0)} [options] addr:port"
opts.on("-n", "--num [num]", "number of keys(default is 1000)"){|v| param[:num] = v.to_i }
opts.on("-v", "--value_size [byte]", "size of each values(default is 1024)"){|v| param[:value_size] = v.to_i }
opts.parse!(ARGV)

if ARGV.length == 0
  STDERR.puts opts.help
  exit
end

begin
  set_sequence(ARGV, param[:num], param[:value_size])
rescue => e
  puts "#{e.class}"
  puts "#{e.message}"
ensure
  puts "#{File.basename($0)} has done."
end
