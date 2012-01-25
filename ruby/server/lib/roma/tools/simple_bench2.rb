#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
require 'date'
require 'roma/client/rclient'

module Roma
  module Client
    class Microbench
      attr :cnt
      attr :tmax
      attr :tmin
      attr :runnable
      attr :sleep_time_watcher
      attr :watcher

      def initialize
        @cnt = 0
        @tmax = 0
        @tmin = 100
        @runnable = true
        @sleep_time_watcher = 10
        init_watcher
      end

      def init_watcher
        @watcher = Thread.new {
          while @runnable
            sleep @sleep_time_watcher
            printf("qps=%d max=%f min=%f ave=%f\n",@@cnt/sleep_time,@@tmax,@@tmin,sleep_time/@@cnt.to_f)
            @cnt = 0
            @tmax = 0
            @tmin = 100
          end
        }
      end
      private :init_watcher

      def send_random_requests_loop addr, port
      end

      def send_read_requests addr, port, count
        rc = Roma::Client::RomaClient.new("#{addr}:#{port.to_s}")
        count.times { |c|
          i = rand count
          ts = DateTime.now
          res = rc.get(i.to_s)
          puts "get k=#{i} #{res}" if res == :error
        }
      end
    end
  end
end


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
