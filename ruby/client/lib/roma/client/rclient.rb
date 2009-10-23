require 'digest/sha1'
require 'socket'
require 'singleton'
require 'roma/commons'
require 'roma/client/client_rttable'
require 'roma/client/sender'

module Roma
  module Client

    # Class to access ROMA .
    class RomaClient

      # ROMA server connect timeout .
      @@timeout = 5

      attr_accessor :rttable
      attr_accessor :sender
      attr_accessor :retry_count_write
      attr_accessor :retry_count_read
      attr_accessor :default_hash_name

      # ROMA client constractor .
      #
      # [ini_nodes] ROMA nodes array
      # [plugin_modules] set plugin modules if you use .
      def initialize(ini_nodes,plugin_modules = nil)
        @retry_count_write = 10
        @retry_count_read = 5
        @default_hash_name = 'roma'

        if plugin_modules
          plugin_modules.each do|plugin|
            self.extend plugin
          end
        end

        init_sender
        update_rttable(ini_nodes.map{|n| n.sub(':','_')})
        init_sync_routing_proc
      end

      def init_sync_routing_proc
        Thread.new {
          begin
            loop {
              sleep 10
              update_rttable(@rttable.nodes)
            }
          rescue => e
            puts "#{e}\n#{$@}"
          end
        }
      end
      private :init_sync_routing_proc

      def init_sender
        @sender = Sender.new
      end
      private :init_sender

      def update_rttable(nodes)
        raise RuntimeError.new("nodes must not be nil.") unless nodes

        nodes.each { |node|
          rt = make_rttable(node)
          if rt != nil
            @rttable = rt
            return
          end
        }

        raise RuntimeError.new("fatal error")
      end

      def make_rttable(node)
        mklhash = @sender.send_route_mklhash_command(node)
        return nil unless mklhash

        if @rttable && @rttable.mklhash == mklhash
          return @rttable
        end

        rd = @sender.send_routedump_command(node)
        if rd
          ret = ClientRoutingTable.new(rd)
          ret.mklhash = mklhash
          return ret
        end
        nil
      rescue
        nil
      end

      # Set value to ROMA .
      # [key] key for store .
      # [value] store value .
      #
      # [return] set status TODO: please write status list .
      def []=(key, value)
        set(key, value)
      end

      # Get value from ROMA .
      # [key] key for roma.
      # <tt>returen</tt>
      #   value sotored roma .
      #   If key don't exit ROMA, return nil .
      #   If coneect error, throw Exception .
      def [](key)
        get(key)
      end

      def set(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "set %s 0 %d %d", expt.to_i, val.length)
      end

      def add(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "add %s 0 %d %d", expt.to_i, val.length)
      end

      def replace(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "replace %s 0 %d %d", expt.to_i, val.length)
      end

      def append(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "append %s 0 %d %d", expt.to_i, val.length)
      end

      def prepend(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "prepend %s 0 %d %d", expt.to_i, val.length)
      end

      def cas(key, val, expt = 0)
        raise RuntimeError.new("Unsupported yet") # TODO
      end

      def delete(key)
        sender(:oneline_receiver, key, nil, "delete %s")
      end

      def out(key)
        sender(:oneline_receiver, key, nil, "out %s")
      end

      def get(key)
        sender(:value_receiver, key, nil, "get %s")[0]
      end

      def gets(keys)
        raise RuntimeError.new("Unsupported yet") # TODO
      end

      def flush_all()
        raise RuntimeError.new("Unsupported yet") # TODO
        @sender.send_flush_all_command
      end

      def incr(key, val = 1)
        ret = sender(:oneline_receiver, key, nil, "incr %s %d", val.to_i)
        return ret if ret =~ /\D/
        ret.to_i
      end

      def decr(key, val = 1)
        ret = sender(:oneline_receiver, key, nil, "decr %s %d", val.to_i)
        return ret if ret =~ /\D/
        ret.to_i
      end

      def stats
        raise RuntimeError.new("Unsupported yet") # TODO
        @sender.send_stats_command
      end

      def version
        raise RuntimeError.new("Unsupported yet") # TODO
        @sender.send_version_command
      end

      def verbosity
        raise RuntimeError.new("Unsupported yet") # TODO
        @sender.send_verbosity_command
      end

      def sender(receiver, key, value ,cmd, *params)
        nid, d = @rttable.search_node(key)
        cmd2 = sprintf(cmd, "#{key}\e#{@default_hash_name}", *params)

        timeout(@@timeout){
          return @sender.send_command(nid, cmd2, value, receiver)
        }
      rescue => e
        unless e.instance_of?(RuntimeError)
          @rttable.proc_failed(nid)
          Roma::Messaging::ConPool.instance.delete_connection(nid)
        end
        sleep 0.3
        retry if (cnt ||= 0; cnt += 1) < @retry_count_write
        raise e
      end

    end

  end
end
