
module Roma
  module Command
    module Definition

      class ClientErrorException < Exception; end
      class ServerErrorException < Exception; end

      def self.included(base)
        # include ClassMethods module into an eigen-class of a +base+
        base.extend ClassMethods
      end

      module ClassMethods

        def def_command_with_relay(cmd, &block)
          # check a duplicated command definition in a same scope
          if public_method_defined? "ev_#{cmd}".to_sym          
            raise "ev_#{cmd} already defined."
          end

          # define a command receiver
          define_method "ev_#{cmd}" do |s|
            begin
              res = {}
              res[@stats.ap_str] = instance_exec(s, &block)
              # command relay
              res.merge! broadcast_cmd("r#{cmd} #{s[1..-1].join(' ')}\r\n")
              send_data("#{res}\r\n")
            rescue ClientErrorException => e
              send_data("CLIENT_ERROR #{e.message}\r\n")
            rescue ServerErrorException => e
              send_data("SERVER_ERROR #{e.message}\r\n")
            rescue LocalJumpError => e
              @log.warn("#{e} #{$@}")
              e.exit_value
            end
          end

          # define a relaid command receiver
          define_method "ev_r#{cmd}" do |s|
            begin
              send_data("#{instance_exec(s, &block)}\r\n")
            rescue ClientErrorException => e
              send_data("CLIENT_ERROR #{e.message}\r\n")
            rescue ServerErrorException => e
              send_data("SERVER_ERROR #{e.message}\r\n")
            rescue LocalJumpError => e
              @log.warn("#{e} #{$@}")
              e.exit_value
            end
          end
        end # def_command_with_relay

        def def_command_with_key(cmd, forward = :one_line, &block)
          define_method "ev_#{cmd}" do |s|
            return send_data("CLIENT_ERROR dose not find key\r\n") if s.length < 2
            begin
              key, hname = s[1].split("\e")
              hname ||= @defhash
              d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
              vn = @rttable.get_vnode_id(d)
              nodes = @rttable.search_nodes_for_write(vn)
              if nodes[0] != @nid
                if forward == :one_line
                  return forward_and_one_line_receive(nodes[0], s)
                elsif  forward == :multi_line
                  return forward_and_multi_line_receive(nodes[0], s)
                end
              end
              instance_exec(s, key, hname, d, vn, nodes, &block)
            rescue ClientErrorException => e
              send_data("CLIENT_ERROR #{e.message}\r\n")
            rescue ServerErrorException => e
              send_data("SERVER_ERROR #{e.message}\r\n")
            rescue LocalJumpError => e
              @log.warn("#{e} #{$@}")
              e.exit_value
            end
          end
        end # def_command_with_key

        def def_command_with_key_value(cmd, idx_of_val_len, forward = :one_line, &block)
          define_method "ev_#{cmd}" do |s|
            return send_data("CLIENT_ERROR dose not find key\r\n") if s.length < 2
            begin
              key, hname = s[1].split("\e")
              hname ||= @defhash
              d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
              vn = @rttable.get_vnode_id(d)
              nodes = @rttable.search_nodes_for_write(vn)
              value = read_bytes(s[idx_of_val_len].to_i)
              read_bytes(2)
              if nodes[0] != @nid
                if forward == :one_line
                  return forward_and_one_line_receive(nodes[0], s, value)
                elsif forward == :multi_line
                  return forward_and_multi_line_receive(nodes[0], s, value)
                end
              end
              instance_exec(s, key, hname, d, vn, nodes, value, &block)
            rescue ClientErrorException => e
              send_data("CLIENT_ERROR #{e.message}\r\n")
            rescue ServerErrorException => e
              send_data("SERVER_ERROR #{e.message}\r\n")
            rescue LocalJumpError => e
              @log.warn("#{e} #{$@}")
              e.exit_value
            end
          end
        end # def_command_with_key_value

      end # module ClassMethods

      #
      def forward_and_one_line_receive(nid, rs, data = nil)
        if rs.last == "forward"
          return send_data("SERVER_ERROR Routing table is inconsistent.\r\n")
        end

        @log.warn("forward #{rs} to #{nid}");

        buf = rs.join(' ') + " forward\r\n"
        buf << data + "\r\n" if data
        res = send_cmd(nid, buf)
        if res == nil || res.start_with?("ERROR")
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        end
        send_data("#{res}\r\n")
      end
      
      # 
      def forward_and_multi_line_receive(nid, rs, data=nil)
        if rs.last == "forward"
          return send_data("SERVER_ERROR Routing table is inconsistent.\r\n")
        end
        
        @log.warn("forward #{rs} to #{nid}");

        buf = rs.join(' ') + " forward\r\n"
        buf << data + "\r\n" if data
        
        con = get_connection(nid)
        con.send(buf)

        buf = con.gets
        if buf == nil
          @rttable.proc_failed(nid)
          @log.error("forward get failed:nid=#{nid} rs=#{rs} #{$@}")
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        elsif buf.start_with?("ERROR")
          @rttable.proc_succeed(nid)
          con.close_connection
          @log.error("forward get failed:nid=#{nid} rs=#{rs} #{$@}")
          return send_data("SERVER_ERROR Message forward failed.\r\n")
        elsif buf.start_with?("VALUE") == false
          return_connection(nid, con)
          @rttable.proc_succeed(nid)
          return send_data(buf)
        end
        
        res = ''
        begin
          res << buf
          s = buf.split(/ /)
          if s[0] != 'VALUE'
            return_connection(nid, con)
            @rttable.proc_succeed(nid)
            return send_data(buf)
          end
          res << con.read_bytes(s[3].to_i + 2)          
        end while (buf = con.gets)!="END\r\n"

        res << "END\r\n"

        return_connection(nid, con)
        @rttable.proc_succeed(nid)

        send_data(res)
      rescue => e
        @rttable.proc_failed(nid) if e.message != "no connection"
        @log.error("forward get failed:nid=#{nid} rs=#{rs} #{e} #{$@}")
        send_data("SERVER_ERROR Message forward failed.\r\n")
      end

    end #  module Definition
  end # module Command
end # module Roma
