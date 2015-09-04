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
    printf("qps=%d max=%f min=%f ave=%f (#{Time.now})\n",@cnt/sleep_time,@tmax,@tmin,sleep_time/@cnt.to_f)
    @cnt=0
    @tmax=0
    @tmin=100
  end
end

def set_counts(rc, range, c)
  range.each do |i|
    ts = DateTime.now
    res=rc.set("key_#{i}","#{c}")
    puts "set k=#{i} #{res}" if res==nil || res.chomp != 'STORED'
    t=(DateTime.now - ts).to_f * 86400.0
    @tmax=t if t > @tmax
    @tmin=t if t < @tmin
    @cnt+=1
  end
end

def check_count(rc, range, c)
  range.each do |i|
    ts = DateTime.now
    res = rc.get("key_#{i}")
    if res != c.to_s
      puts "error k=key_#{i} #{res}" 
    end
    t=(DateTime.now - ts).to_f * 86400.0
    @tmax=t if t > @tmax
    @tmin=t if t < @tmin
    @cnt+=1
  end
end

def test_round
  rc=Roma::Client::RomaClient.new(ARGV)
  n = 0
  loop{
    n += 1
    set_counts(rc, 0...10000, n)
    check_count(rc, 0...10000, n)
    if (n%100 == 0)
      puts "#{n} loop has finished."
    end
  }
end

opts = OptionParser.new
opts.banner = "usage:\r\n  #{File.basename($0)} ${ROMA addr}:${ROMA port}"
opts.parse!(ARGV)

if ARGV.length == 0
  STDERR.puts opts.help
  exit
end

begin
  test_round
rescue => e
  puts "#{e.class} was happened."
  puts "#{e.message}"
ensure
  puts "#{File.basename($0)} has done."
end

