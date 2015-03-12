#!/usr/bin/env ruby
#
# usage:key_list storage-path [param_sleep(sec)]
#
module Roma
  module Storage
  end
  Storage::autoload(:TCStorage,'roma/storage/tc_storage')
  Storage::autoload(:DbmStorage,'roma/storage/dbm_storage')
  Storage::autoload(:SQLite3Storage,'roma/storage/sqlite3_storage')

  class KeyList
    
    def initialize(strgpath, param_sleep)
      each_hash(strgpath){|hname, dir|
        STDERR.puts "### #{hname} #{dir}"
        st = open_storage(dir)

        c = 0
        n = st.true_length
        m = n / 100
        m = 1 if m < 1
        st.divnum.times{|i|
          st.each_hdb_dump(i){|data|
            c += 1
            STDERR.print "#{c}/#{n}\r" if c % m == 0
            vn, last, clk, expt, klen = data.unpack('NNNNN')
            key, = data[20..-1].unpack("a#{klen}")
            STDOUT.puts key
            sleep param_sleep
          }
        }

        st.closedb
        STDERR.puts "\ndone"
      }
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

if ARGV.length < 1
  STDERR.puts "usage:key_list storage-path [param_sleep(sec)]"
  exit
end

if ARGV[1]
  param_sleep = ARGV[1].to_f
else
  param_sleep = 0.001
end

Roma::KeyList.new(ARGV[0], param_sleep)
