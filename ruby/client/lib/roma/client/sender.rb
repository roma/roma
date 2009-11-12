require 'timeout'
require 'yaml'

module Roma
  module Client

    class Sender

      def initialize
      end

      def send_route_mklhash_command(node_id)
        timeout(1){
          conn = Roma::Messaging::ConPool.instance.get_connection(node_id)
          conn.write "mklhash 0\r\n"
          ret = conn.gets
          Roma::Messaging::ConPool.instance.return_connection(node_id, conn)
          return ret.chomp if ret
        }
      rescue =>e
        STDERR.puts "#{node_id} #{e.inspect}"
        return nil
      end

      def send_routedump_command(node_id)
        timeout(1){
          buf = RUBY_VERSION.split('.')
          if buf[0].to_i == 1 && buf[1].to_i == 8
            return send_routedump_yaml_command(node_id)
          end
          conn = Roma::Messaging::ConPool.instance.get_connection(node_id)
          conn.write "routingdump\r\n"
          routes_length = conn.gets.to_i
          if (routes_length <= 0)
            conn.close
            return :error if routes_length < 0
            return nil
          end
          
          routes = ''
          while (routes.length != routes_length)
            routes = routes + conn.read(routes_length - routes.length)
          end
          conn.read(2) # "\r\n"
          conn.gets
          rd = Marshal.load(routes)
          Roma::Messaging::ConPool.instance.return_connection(node_id, conn)
          return rd
        }
      rescue =>e
        STDERR.puts "#{node_id} #{e.inspect}"
        nil
      end

      def send_routedump_yaml_command(node_id)
        conn = Roma::Messaging::ConPool.instance.get_connection(node_id)
        conn.write "routingdump yaml\r\n"
        
        yaml = ''
        while( (line = conn.gets) != "END\r\n" )
          yaml << line
        end

        rd = YAML.load(yaml)
        Roma::Messaging::ConPool.instance.return_connection(node_id, conn)
        return rd        
      end

      def send_stats_command
        # TODO
      end

      def send_version_command(ap)
        conn = Roma::Messaging::ConPool.instance.get_connection(ap)
        conn.write("version\r\n")
        res = conn.gets.chomp
        Roma::Messaging::ConPool.instance.return_connection(ap, conn)
        raise unless res
        return res
      end

      def send_verbosity_command(ap)
        conn = Roma::Messaging::ConPool.instance.get_connection(ap)
        # TODO
        Roma::Messaging::ConPool.instance.return_connection(ap, conn)
      end

      def send_command(nid, cmd, value = nil, receiver = :oneline_receiver)
        con = Roma::Messaging::ConPool.instance.get_connection(nid)
        raise unless con
        if value
          con.write "#{cmd}\r\n#{value}\r\n"
        else
          con.write "#{cmd}\r\n"
        end
        ret = send(receiver, con)
        Roma::Messaging::ConPool.instance.return_connection(nid, con)
        if ret && ret.instance_of?(String) &&
            (ret =~ /^SERVER_ERROR/ || ret =~ /^CLIENT_ERROR/)
          raise ret
        end
        ret
      end

      private

      def oneline_receiver(con)
        ret = con.gets
        ret.chomp if ret
      end

      def value_receiver(con)
        ret = []
        while (line = con.gets) != "END\r\n"
          s = line.split(' ')
          return line.chomp if s[0] == 'SERVER_ERROR' || s[0] == 'CLIENT_ERROR'
          ret << read_bytes(con, s[3].to_i)
          read_bytes(con, 2)
        end
        ret
      end

      def multiplelines_receiver(con) 
        ret = []
        while (line = con.gets) != "END\r\n"
          ret << line.chomp
        end
        ret
      end

      def read_bytes(con, len)
        ret = ''
        until (ret.length == len)
          ret = ret + con.read(len - ret.length)
        end
        ret
      end

    end
    
  end
end
