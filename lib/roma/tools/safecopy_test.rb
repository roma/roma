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

def set_counts(ini_nodes, range, c)
  puts "#{__method__} #{range} #{c}"
  rc=Roma::Client::RomaClient.new(ini_nodes)

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

def check_count(ini_nodes, range, c)
  puts "#{__method__} #{range} #{c}"
  rc=Roma::Client::RomaClient.new(ini_nodes)

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
  set_counts(ARGV, 0...10000, 0)
  Thread.new { random_rquest_sender(ARGV, 10000) }

  nid = ARGV[0]

  sleep(5)

  10.times do |n|
    t = Thread.new { set_counts(ARGV, (n * 1000)...(n * 1000 + 2000), n * 10 + 1) }
    p set_storage_status(nid, n, 'safecopy')
    p wait_status(nid, n, :safecopy_flushed)
    t.join
    p safecopy_stats(nid)
  
    #sleep(30)
    t = Thread.new { set_counts(ARGV, (n * 1000 + 500)...(n * 1000 + 2000), n * 10 + 2) }
    p set_storage_status(nid, n, 'normal')
    p wait_status(nid, n, :normal)
    t.join
    p safecopy_stats(nid)
    check_count(ARGV, (n * 1000)...(n * 1000 + 500), n * 10 + 1)
    check_count(ARGV, (n * 1000 + 500)...(n * 1000 + 2000), n * 10 + 2)
    if (n * 1000 + 2000) < 10000
      check_count(ARGV, (n * 1000 + 2000)...10000, 0)
    end
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

opts.banner = "usage:#{File.basename($0)} [options] addr:port"
opts.parse!(ARGV)

if ARGV.length == 0
  STDERR.puts opts.help
  exit
end

if param.key?(:round)
  test_round
else
  test_change_status
end

puts "#{File.basename($0)} has done."
