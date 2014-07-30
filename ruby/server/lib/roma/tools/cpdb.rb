#!/usr/bin/env ruby

require 'socket'

module Roma
  class SafeCopy

    attr_reader :storages

    def initialize(addr, port)
      @con = TCPSocket.open(addr, port)
      set_gui_run_snapshot_status('true')
      get_storage_info
    end

    def backup_all
      @storages.keys.each do |k|
        backup(k)
      end
    end

    def backup(hname)
      stat = get_safecopy_stats(hname)
      if stat.uniq != [:normal]
        puts "storages[#{hname}].storage.safecopy_stats #{stat.to_s}"
        puts "ERROR: Status except the :normal exists."
        return
      end
      @storages[hname].each_with_index do |fname, num|
        ret = set_storage_status(hname, num, "safecopy")
        if ret != "PUSHED\r\n"
          puts ret
          puts "ERROR: Can't change storage status to safecopy."
          return
        end
        wait(hname, num, :safecopy_flushed)
        puts "copy file : #{fname}"
        # file copy
        `cp #{fname} #{fname}.#{Time.now.strftime("%Y%m%d%H%M%S")}`
        ret = set_storage_status(hname, num, "normal") 
        if ret != "PUSHED\r\n"
          puts ret
          puts "ERROR: Can't change storage status to normal."
          return
        end
        wait(hname, num, :normal)
      end
    end

    def wait(hname, num, stat)
      print "waiting for storages[#{hname}][#{num}] == #{stat} "
      while get_safecopy_stats(hname)[num] != stat
        print "."
        sleep 5
      end
      puts
    end

    def get_storage_info
      @storages = {}
      stats do |line|
        if /^storages\[(.+)\]\.storage\[(\d+)\]\.path\s(.+)/ =~ line
          @storages[$1] = [] unless @storages.key? $1
          @storages[$1][$2.to_i] = $3.chomp
#          puts "#{$1} #{$2} #{$3}"
        end
      end
    end

    def get_safecopy_stats(hname)
      ret = nil
      stats do |line|
        if /^storages\[#{hname}\]\.storage\.safecopy_stats\s(.+)/ =~ line
          ret = $1.chomp
        end
      end
      eval ret
    end

    def set_storage_status(hname, num, stat)
      @con.puts "set_storage_status #{num} #{stat} #{hname}\r\n"
      @con.gets
    end

    def set_gui_run_snapshot_status(status)
      @con.puts "set_gui_run_snapshot #{status}\r\n"
      @con.gets
    end

    def set_gui_last_snapshot
      t = Time.now.strftime('%Y/%m/%dT%H:%M:%S')
      @con.puts "set_gui_last_snapshot #{t}\r\n"
      @con.gets
    end

    def stats
      @con.puts "stat storage\r\n"
      yield $_ while @con.gets != "END\r\n"
    end

    def close
      set_gui_run_snapshot_status('false')
      @con.close if @con
    end

  end # SafeCopy
end # Roma

if ARGV.length < 1
  puts File.basename(__FILE__) + " [port]"
  exit
end

sc = Roma::SafeCopy.new("localhost", ARGV[0].to_i)

begin
  sc.backup_all
  sc.set_gui_last_snapshot
ensure
  sc.close
end
