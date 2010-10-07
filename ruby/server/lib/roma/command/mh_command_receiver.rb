require 'fileutils'
require 'roma/stats'
require 'roma/command/util_command_receiver'

module Roma
  module Command

    module MultiHashCommandReceiver

      # defhash <name>
      def ev_defhash(s)
        res = nil
        if s.length != 2
          res = broadcast_cmd("rdefhash\r\n")
        else
          res = broadcast_cmd("rdefhash #{s[1]}\r\n")
        end
        res[@stats.ap_str] = defhash_event(s)
        send_data("#{res}\r\n")
      end

      # rdefhash <name>
      def ev_rdefhash(s)
        send_data(defhash_event(s)+"\r\n")
      end

      def defhash_event(s)
        if s.length != 2
          "#{@defhash}"
        else
          unless @storages.key?(s[1])
            return "CLIENT_ERROR #{s[1]} dose not find."
          end
          @defhash = s[1]
          "STORED"
        end        
      end
      private :defhash_event

      # mounthash <name>
      def ev_mounthash(s)
        mounthash_event(s) do
          res = broadcast_cmd("rmounthash #{s[1]}\r\n")
          res[@stats.ap_str] = createhash(s[1], 'MOUNTED')
          send_data("#{res}\r\n")
        end
      end

      # rmounthash <name>
      def ev_rmounthash(s)
        mounthash_event(s) do
          send_data("#{createhash(s[1], 'MOUNTED')}\r\n")
        end
      end

      def mounthash_event(s, &block)
        if s.length != 2
          return send_data("CLIENT_ERROR usage:mounthash <name>\r\n")
        end
        if @storages.key?(s[1])
          return send_data("SERVER_ERROR #{s[1]} already mounted.\r\n")
        end
        # check a directory existence
        unless File.directory? "#{Config::STORAGE_PATH}/#{@stats.ap_str}/#{s[1]}"
          return send_data("SERVER_ERROR #{s[1]} dose not find.\r\n")
        end
        block.call
      end
      private :mounthash_event

      # umounthash <name>
      def ev_umounthash(s)
        umounthash_event(s) do
          res = broadcast_cmd("rumounthash #{s[1]}\r\n")
          res[@stats.ap_str] = umounthash(s[1])
          send_data("#{res}\r\n")
        end
      end

      # rumounthash <name>
      def ev_rumounthash(s)
        umounthash_event(s) do
          send_data("#{umounthash(s[1])}\r\n")
        end
      end

      def umounthash_event(s, &block)
        if s.length != 2
          return send_data("CLIENT_ERROR usage:umounthash <name>\r\n")
        end
        unless @storages.key?(s[1])
          return send_data("SERVER_ERROR #{s[1]} dose not find.\r\n")
        end
        block.call
      end
      private :umounthash_event

      # hashlist
      def ev_hashlist(s)
        send_data("#{@storages.keys.join ' '}\r\n")
      end
      
      # createhash <name>
      def ev_createhash(s)
        if s.length != 2
          return send_data("CLIENT_ERROR usage:createhash <name>\r\n")
        end
        res = broadcast_cmd("rcreatehash #{s[1]}\r\n")
        res[@stats.ap_str] = createhash(s[1], 'CREATED')
        send_data("#{res}\r\n")
      end

      # rcreatehash <name>
      def ev_rcreatehash(s)
        if s.length != 2
          return send_data("CLIENT_ERROR usage:createhash <name>\r\n")
        end
        send_data("#{createhash(s[1], 'CREATED')}\r\n")
      end
      
      # deletehash <name>
      def ev_deletehash(s)
        if s.length != 2
          return send_data("CLIENT_ERROR usage:deletehash <name>\r\n")
        end
        res = broadcast_cmd("rdeletehash #{s[1]}\r\n")
        res[@stats.ap_str] = deletehash(s[1])
        send_data("#{res}\r\n")        
      end

      # rdeletehash <name>
      def ev_rdeletehash(s)
        if s.length != 2
          return send_data("CLIENT_ERROR usage:rdeletehash <name>\r\n")
        end
        send_data("#{deletehash(s[1])}\r\n")        
      end

      def createhash(hname, msg)
        if @storages.key?(hname)
          return "SERVER_ERROR #{hname} already exists."
        end
        st = Config::STORAGE_CLASS.new
        st.storage_path = "#{Config::STORAGE_PATH}/#{@stats.ap_str}/#{hname}"
        st.vn_list = @rttable.vnodes
        st.divnum = Config::STORAGE_DIVNUM
        st.option = Config::STORAGE_OPTION
        @storages[hname] = st
        @storages[hname].opendb
        @log.info("createhash #{hname}")
        return msg
      rescue =>e
        @log.error("#{e} #{$@}")
        "NOT #{msg}"
      end
      private :createhash

      def deletehash(hname)
        ret = umounthash(hname)
        return ret if ret != 'UNMOUNTED'
        FileUtils.rm_rf "#{Config::STORAGE_PATH}/#{@stats.ap_str}/#{hname}"
        @log.info("deletehash #{hname}")
        return "DELETED"
      rescue =>e
        @log.error("#{e}")
      end
      private :deletehash
      
      def umounthash(hname)
        if @defhash == hname
          return "SERVER_ERROR default hash can't unmount."
        end
        unless @storages.key?(hname)
          return "SERVER_ERROR #{hname} dose not exists."
        end
        st = @storages[hname]
        @storages.delete(hname)
        st.closedb
        "UNMOUNTED"
      end
      private :umounthash

    end # MultiHashCommandReceiver

  end # module Command
end # module Roma
