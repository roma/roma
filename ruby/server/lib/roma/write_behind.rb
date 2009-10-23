require 'thread'
require 'roma/stats'

module Roma

  module WriteBehind
    
    class FileWriter

      def initialize(path, shift_size, log)
        @stats = Roma::Stats.instance
        path.chop! if path[-1]=='/'
        @path = path
        @log = log
        @fdh = {} # file handle hash
        @fnh = {} # file name hash
        @do_write = false
        @shift_size = shift_size
        @total_size = Hash.new(0)
        @rottime = Time.now
      end

      def get_stat
        ret = {}
        ret['write-behind.path'] = File.expand_path(@path)
        ret['write-behind.shift_size'] = @shift_size
        ret['write-behind.do_write'] = @do_write
        @fdh.each{|hname,fname|
          ret["write-behind[#{hname}].path"] = File.expand_path(fname)
          ret["write-behind[#{hname}].size"] = @total_size[hname]
        }
        ret
      end

      def write(hname, cmd, key, val)
        @do_write = true
        t = Time.now
        if @total_size[hname] >= @shift_size || t >= @rottime
          @do_write = false
          rotate(hname)
        end

        fd = @fdh[hname]
        unless fd
          fd = openfile(hname)
          @total_size[hname] = 0
        end
        klen = key.length
        val = val.to_s
        vlen = val.length
        size = fd.write([t.to_i, cmd, klen, key, vlen, val].pack("NnNa#{klen}Na#{vlen}"))
        @total_size[hname] += size
#        @log.debug("WriteBehind:hname=#{hname} cmd=#{cmd} key=#{key} val=#{val} total_size=#{@total_size}")
      ensure
        @do_write = false
      end

      def rotate(hname)
        @log.info("WriteBehind:rotate #{hname}")
        fd_old = @fdh[hname]
        unless fd_old
          @log.info("WriteBehind:rotate #{hname} not opend")
          return false
        end
        @fdh.delete(hname)
        @fnh.delete(hname)
        sleep 0.01 while @do_write
        fd_old.close
        @log.info("WriteBehind:rotate sccseed")
        true
      end

      def openfile(hname)
        t = Time.now
        path = "#{@path}/#{@stats.ap_str}/#{hname}/#{t.strftime('%Y%m%d')}"
        mkdir(path)
        # set a next rotation time
        @rottime = Time.local(t.year,t.month,t.day,0,0,0) + 24 * 60 * 60

        max = -1
        Dir::glob("#{path}/*.wb").each{|f|
          if /\D(\d+).wb$/ =~ f
            max = $1.to_i if $1.to_i > max
          end
        }
        fname = "#{path}/#{max + 1}.wb"
        fd = open(fname,'wb')
        @fnh[hname] = fname
        @fdh[hname] = fd
      end

      def wb_get_path(hname)
        File.expand_path("#{@path}/#{@stats.ap_str}/#{hname}")
      end

      def get_current_file_path(hname)
        @log.info("WriteBehind:get_current_file_path #{hname}")
        unless @fnh[hname]
          @log.info("WriteBehind:get_current_file_path #{hname} not opend")
          return nil
        end
        File.expand_path("#{@fnh[hname]}")
      end

      def close_all
        @fdh.each_value{|fd| fd.close }
      end

      def mkdir(path)
        pbuf = ''
        path.split('/').each{|p|
          pbuf << p
          begin
            Dir::mkdir(pbuf) unless File.exist?(pbuf)
          rescue
          end
          pbuf << '/'
        }
      end

    end # class FileWriter
    
  end # module WriteBehind

  module WriteBehindProcess

    @@wb_queue = Queue.new

    def self.push(hname, cmd, key, val)
      @@wb_queue.push([hname, cmd, key, val])
    end

    def start_wb_process
      @wb_thread = Thread.new{
        wb_process_loop
      }
    rescue =>e
      @log.error("#{e}\n#{$@}")
    end

    def stop_wb_process
      until @@wb_queue.empty?
        sleep 0.01
      end
      @wb_thread.exit
      @wb_writer.close_all
    end

    def wb_rotate(hname)
      @wb_writer.rotate(hname)
    end

    def wb_get_path(hname)
      @wb_writer.wb_get_path(hname)
    end

    def wb_get_current_file_path(hname)
      @wb_writer.get_current_file_path(hname)
    end

    def wb_get_stat
      @wb_writer.get_stat
    end

    def wb_process_loop
      loop {
        while dat = @@wb_queue.pop
          @wb_writer.write(dat[0], dat[1], dat[2], dat[3])
        end
      }
    rescue =>e
      @log.error("#{e}\n#{$@}")
      retry
    end
    private :wb_process_loop

  end # module WriteBehindProcess

end # module Roma
