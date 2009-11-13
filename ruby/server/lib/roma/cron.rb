require 'roma/stats'
require 'roma/logging/rlogger'
require 'roma/command/receiver'
require 'roma/messaging/con_pool'

module Roma
  
  module Cron

    def initialize_cron
      @stats.crontab = @stats.load_crontab
    end

    def cron
      return unless @stats.crontab
      @stats.crontab.split(/\r?\n/).each { |line|
        next if line[0]=='#'
        mi,h,d,m,w,cmd,*args=line.split(' ')
        begin
          send("cron_#{cmd}",args) if exec_cron?(mi,h,d,m,w)
        rescue =>e
          @log.error("cron:#{e}")
          @log.error(line)
        end
      }
    end

    def exec_cron?(mi,h,d,m,w)
      rightnow=Time.now
      return false unless cronstr_mutch?(0..59,mi,rightnow.min)
      return false unless cronstr_mutch?(0..23,h,rightnow.hour)
      return false unless cronstr_mutch?(1..31,d,rightnow.day)
      return false unless cronstr_mutch?(1..12,m,rightnow.month)
      return false unless cronstr_mutch?(0..6,d,rightnow.wday)
      true
    end

    # TODO
    def cronstr_mutch?(rng,s,i)
      raise "parse exception" unless s
      ss=s.split(/\//)
      if(ss.length==2)
        if i % ss[1].to_i == 0
          return true
        else
          return false
        end
      end
      return true if s=='*'
      s.to_i==i.to_i
    end

    def cron_test(args)
      @log.debug("corn_test #{Time.now} #{args}")
    end

  end # module Cron
end # module Roma
