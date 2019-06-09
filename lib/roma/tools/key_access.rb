#!/usr/bin/env ruby
#
require 'optparse'

module Roma
  module Storage
  end
  Storage::autoload(:TCStorage,'roma/storage/tokyocabinet')
  Storage::autoload(:DbmStorage,'roma/storage/dbm')
  Storage::autoload(:SQLite3Storage,'roma/storage/sqlite3')

  class KeyAccess

    def initialize(argv)
      options(argv)

      each_hash(@path){|hname, dir|
        puts "hash : #{hname}"
        st = open_storage(dir)

        vn, last, clk, expt, value = st.get_raw2(@key)
        if vn
          if @sv
            puts "vnode: #{vn}"
            puts "last : #{Time.at(last)}"
            puts "clock: #{clk}"
            puts "expt : #{Time.at(expt)}"
            begin
              puts "value #{Marshal.load(value)}"
            rescue
              puts "value: #{value}"
            end
          else
            puts "exist"
          end
          st.closedb
          return
        end

        st.closedb
      }
      puts "not exist"
    end

    def options(argv)
      opts = OptionParser.new
      opts.banner="usage:#{File.basename($0)} storage-path key"

      @sv = false
      opts.on("-v","--value","show value") { |v| @sv = true }

      opts.parse!(argv)
      raise OptionParser::ParseError.new if argv.length < 2
      @path = argv[0]
      @key = argv[1]
    rescue OptionParser::ParseError => e
      STDERR.puts opts.help
      exit 1
    end

    def each_hash(path)
      Dir::glob("#{path}/*").each{|dir|
        next unless File::directory?(dir)
        hname = dir[dir.rindex('/')+1..-1]
        yield hname,dir
      }
    end

    def open_storage(path)
      unless File::directory?(path)
        STDERR.puts "#{path} does not found."
        return nil
      end

      # get a file extension
      ext = File::extname(Dir::glob("#{path}/0.*")[0])[1..-1]
      # count a number of divided files
      divnum = Dir::glob("#{path}/*.#{ext}").length

      st = new_storage(ext)
      st.divnum = divnum
      st.vn_list = []
      st.storage_path = path
      st.opendb
      st
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

  end

end

Roma::KeyAccess.new(ARGV)
