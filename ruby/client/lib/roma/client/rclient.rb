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
          if rt
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
      # please see set method .
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

      # Set value to ROMA .
      #
      # Both same same key exists or not exists in ROMA, this method set value .
      #
      # [key] key for store .
      # [value] store value .
      # [exp] expire seconds .
      #
      # [return] return follow set status .
      # - If method is success, return STORED .
      # - If method is not stored, return NOT_STORED .
      # - If server error, return SERVER_ERROR .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def set(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "set %s 0 %d %d", expt.to_i, val.length)
      end

      # Add value to ROMA .
      #
      # If same key exists in ROMA, this method don't overwrite value
      # and return NOT_STORED .
      #
      # [key] key for store .
      # [value] store value .
      # [exp] expire seconds .
      #
      # [return] return follow set status .
      # - If method is success, return STORED .
      # - If same key exists in ROMA, return NOT_STORED .
      # - If server error, return SERVER_ERROR .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def add(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "add %s 0 %d %d", expt.to_i, val.length)
      end

      # Add value to ROMA .
      #
      # If same key exists in ROMA, this method overwrite value .
      # If same key doesn't exist in ROMA this method don't store value and
      # return NOT_STORE .
      #
      # [key] key for store .
      # [value] store value .
      # [exp] expire seconds .
      #
      # [return] return follow set status .
      # - If method is success, return STORED .
      # - If same key exists in ROMA, return NOT_STORED .
      # - If server error, return SERVER_ERROR .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def replace(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "replace %s 0 %d %d", expt.to_i, val.length)
      end

      # Append value to exists string .
      #
      # If same key exists in ROMA, this method append value .
      # If same key doesn't exist in ROMA this method don't store value and
      # return NOT_STORE .
      #
      # [key] key for append .
      # [value] append value .
      # [exp] expire seconds .
      #
      # [return] return follow set status .
      # - If method is success, return STORED .
      # - If same key exists in ROMA, return NOT_STORED .
      # - If server error, return SERVER_ERROR .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def append(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "append %s 0 %d %d", expt.to_i, val.length)
      end

      # Prepend value to exists string .
      #
      # If same key exists in ROMA, this method prepend value .
      # If same key doesn't exist in ROMA this method don't store value and
      # return NOT_STORE .
      #
      # [key] key for prepend .
      # [value] prepend value .
      # [exp] expire seconds .
      #
      # [return] return follow set status .
      # - If method is success, return STORED .
      # - If same key exists in ROMA, return NOT_STORED .
      # - If server error, return SERVER_ERROR .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def prepend(key, val, expt = 0)
        sender(:oneline_receiver, key, val, "prepend %s 0 %d %d", expt.to_i, val.length)
      end

      def cas(key, val, expt = 0)
        raise RuntimeError.new("Unsupported yet") # TODO
      end

      # Delete value .
      #
      # [key] key for delete .
      #
      # [return] return follow set status .
      # - If method is success, return DELETED .
      # - If same key doesn't exist in ROMA, return NOT_FOUND .
      # - If server error, return SERVER_ERROR .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def delete(key)
        sender(:oneline_receiver, key, nil, "delete %s")
      end

      # Delete value completely .
      #
      # This method delete value completely. "completely" means
      # Don't set delete flag in server, but delete value in storage .
      # Delete method set delete flag, but delete value soon .
      #
      # [key] key for delete .
      #
      # [return] return follow set status .
      # - If method is success, return DELETED .
      # - If same key doesn't exist in ROMA, return NOT_FOUND .
      # - If server error, return SERVER_ERROR .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def out(key)
        sender(:oneline_receiver, key, nil, "out %s")
      end

      # get value
      #
      # [key] key for get .
      #
      # [return] return stored value in ROMA .
      # If key doesn't exist in ROMA, this method return nil .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def get(key)
        sender(:value_list_receiver, key, nil, "get %s")[0]
      end

      # get values .
      #
      # [keys] key array for get .
      #
      # [return] return key and sotored value hash .
      # If all key doesn't exist in ROMA, return empty hash .
      # If some key doesn't exist in ROMA, return exist key and sotred value hash .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def gets(keys)
        kn = {}
        keys.each{|key|
          nid, d = @rttable.search_node(key)
          kn[nid] ||= []
          kn[nid] << key
        }

        res = {}
        kn.each_pair{|nid,ks|
          res.merge!(gets_sender(nid, ks))
        }
        res
      end

      def flush_all()
        raise RuntimeError.new("Unsupported yet") # TODO
        @sender.send_flush_all_command
      end

      # increment value .
      #
      # [key] key for incremental .
      # [val] incremental value .
      #
      # [return] Fixnum incrementaled value .
      # If same key doesn't exist in ROMA, return NOT_FOUND .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
      def incr(key, val = 1)
        ret = sender(:oneline_receiver, key, nil, "incr %s %d", val.to_i)
        return ret if ret =~ /\D/
        ret.to_i
      end

      # decrement value .
      #
      # [key] key for decremental .
      # [val] decremental value .
      #
      # [return] Fixnum decrementaled value .
      # If same key doesn't exist in ROMA, return NOT_FOUND .
      #
      # If socket error occured, throw Exception .
      #
      # If socket timeout occured, throw TimeoutError .
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

      private

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

      def gets_sender(nid, keys)
        cmd = "gets"
        keys.each{ |k|
          cmd << " #{k}\e#{@default_hash_name}"
        }

        timeout(@@timeout){
          return @sender.send_command(nid, cmd, nil, :value_hash_receiver)
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

    end # class RomaClient

  end # module Client
end # module Roma
