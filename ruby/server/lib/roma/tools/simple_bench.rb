#!/usr/bin/env ruby
require 'date'
require 'roma/client/rclient'

@@cnt=0
@@tmax=0
@@tmin=100

Thread.new {
  sleep_time=10
  while(true)
    sleep sleep_time
    printf("qps=%d max=%f min=%f ave=%f\n",@@cnt/sleep_time,@@tmax,@@tmin,sleep_time/@@cnt.to_f)
    @@cnt=0
    @@tmax=0
    @@tmin=100
  end
}

def random_rquest_sender(ini_nodes)
  rc=Roma::Client::RomaClient.new(ini_nodes)

  n=10000
  loop{
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
    @@tmax=t if t > @@tmax
    @@tmin=t if t < @@tmin
    @@cnt+=1
  }
end
 
if ARGV.length == 0
  STDERR.puts "usage:simple_bench addr:port"
  exit
end

tn=10
t=[]
tn.times{
  t << Thread.new{
    random_rquest_sender(ARGV)
  }
}

t[0].join

