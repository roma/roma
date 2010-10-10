
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
            end
          end
        end # def_command_with_relay

        def def_command_with_key(cmd, &block)
          define_method "ev_#{cmd}" do |s|
            return send_data("CLIENT_ERROR dose not find key\r\n") if s.length < 2
            begin
              key, hname = s[1].split("\e")
              hname ||= @defhash
              d = Digest::SHA1.hexdigest(key).hex % @rttable.hbits
              vn = @rttable.get_vnode_id(d)
              nodes = @rttable.search_nodes_for_write(vn)
              instance_exec(s, key, hname, d, vn, nodes, &block)
            rescue ClientErrorException => e
              send_data("CLIENT_ERROR #{e.message}\r\n")
            rescue ServerErrorException => e
              send_data("SERVER_ERROR #{e.message}\r\n")
            end
          end
        end # def_command_with_key

        def def_command_with_key_value(cmd, idx_of_val_len, &block)
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
              instance_exec(s, key, hname, d, vn, nodes, value, &block)
            rescue ClientErrorException => e
              send_data("CLIENT_ERROR #{e.message}\r\n")
            rescue ServerErrorException => e
              send_data("SERVER_ERROR #{e.message}\r\n")
            end
          end
        end # def_command_with_key_value

      end # module ClassMethods

    end #  module Definition
  end # module Command
end # module Roma
