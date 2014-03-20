#!/usr/bin/env ruby
require 'optparse'
require 'date'
require 'roma/client/rclient'

@cnt = 0
@tmax = 0
@tmin = 100

@m = Mutex.new

Thread.new do
  sleep_time=10
  while(true)
    sleep sleep_time
    printf("\s\sqps=%d max=%f min=%f ave=%f\n",@cnt/sleep_time,@tmax,@tmin,sleep_time/@cnt.to_f)
    @cnt=0
    @tmax=0
    @tmin=100
  end
end

def random_rquest_sender(ini_nodes, n)
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

def set_counts(ini_nodes, range, key_prefix, value)
  puts "\s\s#{__method__} #{range} #{value}"
  rc=Roma::Client::RomaClient.new(ini_nodes)
  @range_cnt = 0

  range.each do |i|
    ts = DateTime.now
    @range_cnt = i
    res=rc.set("#{key_prefix}_#{i}","#{value}")
    puts "set #{key_prefix}_#{i}=#{value} #{res}" if res==nil || res.chomp != 'STORED'
    t=(DateTime.now - ts).to_f * 86400.0
    @tmax=t if t > @tmax
    @tmin=t if t < @tmin
    @cnt+=1
  end
end

def check_count(ini_nodes, range, key_prefix, value)
  puts "\s\s#{__method__} #{range} #{value}"
  rc=Roma::Client::RomaClient.new(ini_nodes)

  range.each do |i|
    ts = DateTime.now
    res = rc.get("#{key_prefix}_#{i}")
    if res != value.to_s
      puts "error k=#{key_prefix}_#{i} #{res}" 
    end

    t=(DateTime.now - ts).to_f * 86400.0
    @tmax=t if t > @tmax
    @tmin=t if t < @tmin
    @cnt+=1
  end
end

def send_cmd(nid, cmd)
  conn = Roma::Client::ConPool.instance.get_connection(nid)
  conn.write "#{cmd}\r\n"
  ret = conn.gets
  Roma::Client::ConPool.instance.return_connection(nid, conn)
  ret
rescue =>e
  STDERR.puts "#{nid} #{cmd} #{e.inspect}"
  nil
end

def stats(nid, regexp=nil)
  conn = Roma::Client::ConPool.instance.get_connection(nid)
  if regexp
    conn.write "stats #{regexp}\r\n"
  else
    conn.write "stats\r\n"
  end
  ret = ""
  while(conn.gets != "END\r\n")
    ret << $_
  end
  Roma::Client::ConPool.instance.return_connection(nid, conn)
  ret
rescue =>e
  STDERR.puts "#{nid} #{e.inspect}"
  nil  
end

def safecopy_stats(nid)
  ret = stats(nid, 'storage.safecopy_stats')
  return eval $1 if ret =~ /^.+\s(\[.+\])/
  nil
end

def set_storage_status(nid, fno, stat)
  send_cmd(ARGV[0], "set_storage_status #{fno} #{stat}")
end

def wait_status(nid, fno, stat)
  while safecopy_stats(nid)[fno] != stat
    sleep 5
  end
  stat
end

def test_change_status

  puts "write (0...10000) = 0"
  set_counts(ARGV, 0...10000, "default_key",0)
  Thread.new { random_rquest_sender(ARGV, 10000) }

  set_counts(ARGV, 0...1000, "flushing_key", 0)
  set_counts(ARGV, 0...1000, "caching_key", 0)

  nid = ARGV[0]

  sleep(5)

  10.times do |n|
    puts "\n#{n+1}th loop(#{n}.tc)****************************************************************** " 

    #========================================================================================
    #flushing(normal => safecopy_flushed)
    flush_loop_count = 0
    @range_cnt = 0
    @flag = false

    t = Thread.new {
      loop{
        flush_loop_count += 1
        set_counts(ARGV, 0...1000, "flushing_key", flush_loop_count)
        @flag = true
      }
    }
    while !@flag do
      puts "\s\s[debug]sleep flushing start"
      sleep(1)
      puts "\s\s[debug]sleep flushing end"
    end
    puts "\s\s#{set_storage_status(nid, n, 'safecopy')}"
    puts "#{wait_status(nid, n, :safecopy_flushed)}"

    #sleep(5)
    t.kill

    flushing_range_cnt = @range_cnt
    puts "\s\s#{safecopy_stats(nid)}\n\n"

    #========================================================================================
    #Caching(safecopy_flushed => normal)
    #sleep(30)
    cache_loop_count = 0
    @range_cnt = 0
    @flag = false
    t = Thread.new {
      loop{
        cache_loop_count += 1
        set_counts(ARGV, 0...1000, "caching_key", cache_loop_count)
        @flag = true
      }
    }
    while !@flag do 
      puts "\s\s[debug]sleep caching start"
      sleep(1)
      puts "\s\s[debug]sleep caching end"
    end

    puts "\s\s#{set_storage_status(nid, n, 'normal')}"
    puts "#{wait_status(nid, n, :normal)}"
    
    #sleep(5)
    t.kill   

    caching_range_cnt = @range_cnt
    puts "\s\s#{safecopy_stats(nid)}"

    #========================================================================================
    #check
    puts "\n[Check]"
    puts "\s\sflushing key"
    check_count(ARGV, 0..flushing_range_cnt, "flushing_key", flush_loop_count)
    check_count(ARGV, flushing_range_cnt+1...1000, "flushing_key", flush_loop_count-1)

    puts "\n\s\scaching key"
    check_count(ARGV, 0..caching_range_cnt, "caching_key", cache_loop_count)
    check_count(ARGV, caching_range_cnt+1...1000, "caching_key", cache_loop_count-1) if cache_loop_count != 1
  end
end

def test_round
  n = 0
  1000.times do |i|
    set_counts(ARGV, 0...10000, i)
    check_count(ARGV, 0...10000, i)
  end
end

param = { :num=>10000, :th=>1 }

opts = OptionParser.new

opts.on("-r", "--round", "round request"){|v| param[:round] = v }
opts.on("-c", "--count [x]", "counts of the test times"){|v| param[:count] = v.to_i }

opts.banner = "usage:#{File.basename($0)} [options] addr:port"
opts.parse!(ARGV)

if ARGV.length == 0
  STDERR.puts opts.help
  exit
end

if param.key?(:round)
  test_round
else
  param[:count] = 1 if !param.key?(:count)

  param[:count].times do |count|
    puts "#{count+1}th test========================================================================================="
    test_change_status
  end
end

puts "#{File.basename($0)} has done."
