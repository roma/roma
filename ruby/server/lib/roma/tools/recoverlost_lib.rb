#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
#
# usage:recoverlost address port storage-path [yyyymmddhhmmss]
#
require 'roma/client/sender'
require 'roma/messaging/con_pool'
require 'roma/routing/routing_data'

module Roma
  module Storage
  end
  Storage::autoload(:TCStorage,'roma/storage/tc_storage')
  Storage::autoload(:DbmStorage,'roma/storage/dbm_storage')
  Storage::autoload(:SQLite3Storage,'roma/storage/sqlite3_storage')

  class RecoverLost
    
    def initialize(pname, pushv_cmd, argv)
      if argv.length < 3
        puts "usage:#{pname} address port storage-path [yyyymmddhhmmss]"
        exit
      end

      @addr = argv[0]
      @port = argv[1]
      @strgpath = argv[2]
      @ymdhms = argv[3]

      if @port =~ /\D/
        STDERR.puts "port was not numeric."
        exit
      end
      
      if @ymdhms && (@ymdhms.length != 14 || @ymdhms =~ /\D/)
        STDERR.puts "yyyymmddhhmmss format mismatch."
        exit
      end
      @pushv_cmd = pushv_cmd
      @nodeid = "#{@addr}_#{@port}"
      @stream_copy_wait_param = 0.0001
    end

    def suite
      @rd = get_routing_data(@nodeid)
      @lost_vnodes = get_lost_vnodes(@rd,@ymdhms)
      puts "#{@lost_vnodes.length} vnodes where data was lost."

      exit if @lost_vnodes.length == 0

      each_hash(@strgpath){|hname,dir|
        puts "#{hname} #{dir}"
        @storage = open_storage(dir,@lost_vnodes)
        start_recover(hname)
        @storage.closedb
      }
    end

    def each_hash(path)
      Dir::glob("#{path}/*").each{|dir|
        next unless File::directory?(dir)
        hname = dir[dir.rindex('/')+1..-1]
        yield hname,dir
      }      
    end

    def get_routing_data(nid)
      sender = Roma::Client::Sender.new
      sender.send_routedump_command(nid)
    end

    def get_lost_vnodes(rd,ymdhms)
      ret = rd.get_lost_vnodes
      if ymdhms
        ret |= get_history_of_lost(rd.nodes[0],ymdhms)
      end
      ret
    end

    def get_history_of_lost(nid,ymdhms)
      ret = []
      con = Roma::Messaging::ConPool.instance.get_connection(nid)
      con.write("history_of_lost #{ymdhms}\r\n")
      while((buf = con.gets) != "END\r\n")
        ret << buf.chomp.to_i
      end
      Roma::Messaging::ConPool.instance.return_connection(nid, con)
      ret
    end

    def open_storage(path,vn_list)
      unless File::directory?(path)
        STDERR.puts "#{path} dose not found."
        return nil
      end

      # get a file extension
      ext = File::extname(Dir::glob("#{path}/0.*")[0])[1..-1]
      # count a number of divided files
      divnum = Dir::glob("#{path}/*.#{ext}").length
        
      st = new_storage(ext)
      st.divnum = divnum
      st.vn_list = vn_list
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

    def start_recover(hname)
      @lost_vnodes.each{|vn|
        nodes = @rd.v_idx[vn]
        if nodes == nil || nodes.length == 0
          nid = @rd.nodes[rand(@rd.nodes.length)]
          puts "#{vn} assign to #{nid}"
        else
          nid = nodes[0]
          puts "#{vn} was auto assirned at #{nid}"
        end

        if push_a_vnode_stream(hname, vn, nid)!="STORED"
          STDERR.puts "push_a_vnode_stream aborted in #{vn}"
          exit
        end

        if nodes == nil || nodes.length == 0
          cmd = "setroute #{vn} #{@rd.v_clk[vn]} #{nid}\r\n"
          exit unless send_cmd(nid ,cmd)
          broadcast_cmd(cmd, nid)
        end
      }
    end

    def push_a_vnode(hname, vn, nid)
      dmp = @storage.dump(vn)
      return true unless dmp
      con = Roma::Messaging::ConPool.instance.get_connection(nid) unless con
      con.write("pushv #{hname} #{vn}\r\n")
      res = con.gets
      con.write("#{dmp.length}\r\n#{dmp}\r\nEND\r\n")
      res = con.gets
      con.close
      res.chomp! if res
      res
    rescue =>e
      STDERR.puts "#{e}\n#{$@}"
      nil
    end

    def push_a_vnode_stream(hname, vn, nid)
      con = Roma::Messaging::ConPool.instance.get_connection(nid)

#      con.write("spushv #{hname} #{vn}\r\n")
      con.write("#{@pushv_cmd} #{hname} #{vn}\r\n")

      res = con.gets # READY\r\n or error string
      if res != "READY\r\n"
        con.close
        return res.chomp
      end

      @storage.each_vn_dump(vn){|data|
        con.write(data)
        sleep @stream_copy_wait_param
      }
      con.write("\0"*20) # end of steram

      res = con.gets # STORED\r\n or error string
      Roma::Messaging::ConPool.instance.return_connection(nid,con)
      res.chomp! if res
      res
    rescue =>e
      STDERR.puts "#{e}\n#{$@}"
      nil
    end

    def broadcast_cmd(cmd,without_nids=nil)
      without_nids=[] unless without_nids
      res = {}
      @rd.nodes.each{ |nid|
        res[nid] = send_cmd(nid,cmd) unless without_nids.include?(nid)
      }
      res
    rescue => e
      STDERR.puts("#{e}\n#{$@}")
      nil
    end

    def send_cmd(nid, cmd)
      con = Roma::Messaging::ConPool.instance.get_connection(nid)
      con.write(cmd)
      res = con.gets
      Roma::Messaging::ConPool.instance.return_connection(nid, con)
      if res
        res.chomp!
      end
      res
    rescue => e
      STDERR.puts("#{__FILE__}:#{__LINE__}:Send command failed that node-id is #{nid},command is #{cmd}.")
      nil
    end

  end # class RecoverLost
end # module Roma
