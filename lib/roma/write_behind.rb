require 'thread'
require 'roma/stats'

require 'socket'

module Roma

  module WriteBehind
    
    class FileWriter

      attr_accessor :shift_size

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
          @log.info("WriteBehind file has been created: [#{@fnh[hname]}]")
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
        @log.info("WriteBehind:rotate succeed")
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

    class StreamWriter

      attr_accessor :run_replication
      attr_accessor :replica_mklhash
      attr_accessor :replica_nodelist
      attr_accessor :replica_rttable

      def initialize(log)
        @log = log
        @run_replication = false
        @replica_mklhash = nil
        @replica_nodelist = []
        @replica_rttable = nil
        #@stats = Roma::Stats.instance
      end

      def get_stat
        ret = {}
        ret['write-behind.run_replication'] = @run_replication
        ret['write-behind.replica_mklhash'] = @replica_mklhash
        ret['write-behind.replica_nodelist'] = @replica_nodelist
        ret['write-behind.replica_rttable'] = @replica_rttable
        ret
      end

      def change_mklhash?
        addr, port = @replica_nodelist[0].split(/[:_]/)
        con = TCPSocket.open(addr, port)
        con.write("mklhash 0\r\n")
        current_mklhash = con.gets.chomp
        con.close
        if current_mklhash == @replica_mklhash
          return false
        else
          return true
        end
      rescue
        @replica_nodelist.shift
        if @replica_nodelist.length == 0
          @run_replication = false
          @log.error("Replicate Cluster was down.")
        else
          retry
        end
      end

      def update_mklhash(nid)
        addr, port = nid.split(/[:_]/)
        con = TCPSocket.open(addr, port)
        con.write("mklhash 0\r\n")
        @replica_mklhash = con.gets.chomp
        con.close
        @log.debug("replica_mklhash has updated: [#{@replica_mklhash}]")
      end

      def update_nodelist(nid)
        addr, port = nid.split(/[:_]/)
        con = TCPSocket.open(addr, port)
        con.write("nodelist\r\n")
        @replica_nodelist = con.gets.chomp.split("\s")
        con.close
        @log.debug("replica_nodelist has updated: #{@replica_nodelist}")
      end

      def update_rttable(nid)
        'toDO'
      end
    end # class StreamWriter
    
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
      @wb_thread[:name] = 'write_behind'
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
      @wb_writer.get_stat.merge(@cr_writer.get_stat)
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
