#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
#
# usage:mkrecent dgst-bits div-bits divnum storage-path1 storage-path2 recent-storage-path
#
require 'roma/routing/routing_data'

module Roma
  module Storage
  end
  Storage::autoload(:TCStorage,'roma/storage/tc_storage')
  Storage::autoload(:DbmStorage,'roma/storage/dbm_storage')
  Storage::autoload(:SQLite3Storage,'roma/storage/sqlite3_storage')

  class MakeRecentData
    
    def initialize(argv = nil)
      if argv.length != 6
        STDERR.puts "usage:mkrecent dgst-bits div-bits divnum storage-path1 storage-path2 recent-storage-path"
        exit
      end

      dgst_bits = argv[0].to_i
      div_bits = argv[1].to_i
      @divnum = argv[2].to_i
      @strgpath1 = argv[3]
      @strgpath2 = argv[4]
      @recentpath = argv[5]

      @vnodes = []
      (2**div_bits).times{|i|
        @vnodes << ( i<<(dgst_bits-div_bits) )
      }
    end

    def suite
      if File::directory?(@recentpath)
        STDERR.puts "#{@recentpath} exists."
        exit
      end
      
      Dir::mkdir(@recentpath)

      Dir::glob("#{@strgpath1}/*").each{|dir|
        next unless File::directory?(dir)
        hname = dir[dir.rindex('/')+1..-1]
        open_storage(dir,
                     "#{@strgpath2}/#{hname}",
                     "#{@recentpath}/#{hname}")
        exec(hname)

        close_storage
      }
    end

    def open_storage(p1,p2,rp)
      puts "Open #{p1}"
      @st1 = ropen(p1)
      @st1.each_vn_dump_sleep = 0
      exit unless @st1
      puts "Open #{p2}"
      @st2 = ropen(p2)
      @st2.each_vn_dump_sleep = 0
      unless @st2
        STDERR.puts ""
        @st1.closedb
        exit
      end
 
      if @st1.class != @st2.class
        STDERR.puts "#{p1} and #{p2} that file type is different."
        @st1.closedb
        @st2.closedb
        exit
      end

      puts "Open #{rp}"
      @rst = @st1.class.new
      @rst.divnum = @divnum
      @rst.vn_list = @vnodes
      @rst.storage_path = rp
      @rst.opendb
    end

    def close_storage
      @st1.closedb
      @st2.closedb
      @rst.closedb
    end

    def exec(hname)
      n = 0
      @vnodes.each{|vn|
        printf "#{hname}:#{n}/#{@vnodes.length}\r"
        n+=1
        buf = @st1.dump(vn)
        @rst.load( buf ) if buf
        buf = @st2.dump(vn)
        @rst.load( buf ) if buf
      }
    end

    private

    def ropen(path)
      unless File::directory?(path)
        STDERR.puts "#{path} dose not found."
        return nil
      end

      ext = File::extname(Dir::glob("#{path}/0.*")[0])[1..-1]

      storage = new_storage(ext)
      storage.divnum = @divnum
      storage.vn_list = @vnodes
      storage.storage_path = path
      storage.opendb
      storage
    end

    def new_storage(ext)
      case(ext)
      when 'tc'
        return ::Roma::Storage::TCStorage.new
      when 'dbm'
        return Roma::Storage::DbmStorage.new
      when 'sql3'
        return Roma::Storage::SQLite3Storage.new
      else
        return nil
      end
    end

  end # MakeRecentData
end # module Roma

Roma::MakeRecentData.new(ARGV).suite
puts "Make recent data process has succeed."
