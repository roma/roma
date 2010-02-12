require 'roma/stats'
require 'roma/command/util_command_receiver'

module Roma
  module Command

    module MultiHashCommandReceiver

      # defhash hashname
      def ev_defhash(s)
        if s.length!=2
          send_data("CLIENT_ERROR usage:defhash hashname\r\n")
          return
        end
        @defhash=s[1]
        send_data("STORED\r\n")
      end

      # hashlist
      def ev_hashlist(s)
        ret=''
        @storages.each_key{|hn| ret << hn << ' ' }
        send_data("#{ret[0...-1]}\r\n")
      end
      
      # createhash hashname
      def ev_createhash(s)
        if s.length != 2
          send_data("CLIENT_ERROR usage:createhash hashname\r\n")
          return
        end
        res = broadcast_cmd("rcreatehash #{s[1]}\r\n")
        res[@stats.ap_str] = createhash(s[1])
        send_data("#{res.inspect}\r\n")
      end

      # rcreatehash hashname
      def ev_rcreatehash(s)
        if s.length != 2
          send_data("CLIENT_ERROR usage:createhash hashname\r\n")
          return
        end
        send_data("#{createhash(s[1])}\r\n")
      end
      
      def createhash(hname)
        if @storages.key?(hname)
          return "SERVER_ERROR #{hname} already exists."
        end
        st = Roma::Config::STORAGE_CLASS.new
        st.storage_path = "#{Roma::Config::STORAGE_PATH}/#{@stats.ap_str}/#{hname}"
        st.vn_list = @rttable.vnodes
        st.divnum = Roma::Config::STORAGE_DIVNUM
        st.option = Roma::Config::STORAGE_OPTION
        @storages[hname] = st
        @storages[hname].opendb
        @log.info("createhash #{hname}")
        return "CREATED"
      rescue =>e
        @log.error("#{e}")
      end
      private :createhash

      # deletehash hashname
      def ev_deletehash(s)
        if s.length != 2
          send_data("CLIENT_ERROR usage:deletehash hashname\r\n")
          return
        end
        res = broadcast_cmd("rdeletehash #{s[1]}\r\n")
        res[@stats.ap_str] = deletehash(s[1])
        send_data("#{res.inspect}\r\n")        
      end

      # rdeletehash hashname
      def ev_rdeletehash(s)
        if s.length != 2
          send_data("CLIENT_ERROR usage:rdeletehash hashname\r\n")
          return
        end
        send_data("#{deletehash(s[1])}\r\n")        
      end

      def deletehash(hname)
        unless @storages.key?(hname)
          return "SERVER_ERROR #{hname} dose not exists."
        end
        if hname == 'roma'
          return "SERVER_ERROR the hash name of 'roma' can't delete."
        end
        st = @storages[hname]
        @storages.delete(hname)
        st.closedb
        rm_rf("#{Roma::Config::STORAGE_PATH}/#{@stats.ap_str}/#{hname}")
        @log.info("deletehash #{hname}")
        return "DELETED"
      rescue =>e
        @log.error("#{e}")
      end
      private :deletehash
      
      # looked like a "rm -rf" command
      def rm_rf(fname)
        return unless File.exists?(fname)
        if File::directory?(fname)
          Dir["#{fname}/*"].each{|f| rm_rf(f) }
          Dir.rmdir(fname)
        else
          File.delete(fname)
        end
      end
      private :rm_rf

    end # MultiHashCommandReceiver

  end # module Command
end # module Roma
