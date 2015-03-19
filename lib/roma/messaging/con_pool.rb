require 'thread'
require 'socket'
require 'singleton'
require 'roma/dns_cache'

module Roma
  module Messaging
    
    class ConPool
      include Singleton

      attr_accessor :maxlength
      attr_accessor :expire_time

      def initialize(maxlength = 10, expire_time = 30)
        @pool = {}
        @maxlength = maxlength
        @expire_time = expire_time
        @lock = Mutex.new
      end

      def get_connection(ap)
        ret,last = @pool[ap].shift if @pool.key?(ap) && @pool[ap].length > 0
        if ret && @expire_time != 0 && last < Time.now - @expire_time
          ret.close
          ret = nil
          Logging::RLogger.instance.info("connection expired at #{ap},remains #{@pool[ap].length}")
        end
        return create_connection(ap) unless ret
        ret
      rescue => e
        Logging::RLogger.instance.error("#{__FILE__}:#{__LINE__}:#{e}")
        nil
      end

      def check_connection(ap)
        host, port = ap.split(/[:_]/)
        addr = DNSCache.instance.resolve_name(host)
        TCPSocket.open(addr, port)
        true
      rescue => e
        false
      end

      def return_connection(ap, con)
        if select([con],nil,nil,0.0001)
          con.gets
          con.close
          return
        end

        if @pool.key?(ap) && @pool[ap].length > 0
          if @pool[ap].length > @maxlength
            con.close
          else
            @pool[ap] << [con, Time.now]
          end
        else
          @pool[ap] = [[con, Time.now]]
        end
      rescue => e
        Logging::RLogger.instance.error("#{__FILE__}:#{__LINE__}:#{e}")
      end

      def create_connection(ap)
        host, port = ap.split(/[:_]/)
        addr = DNSCache.instance.resolve_name(host)
        TCPSocket.new(addr, port)
      end

      def delete_connection(ap)
        @pool.delete(ap)
      end

      def close_all
        @pool.each_key{|ap| close_at(ap) }
      end

      def close_same_host(ap)
        host,port = ap.split(/[:_]/)
        @pool.each_key{|eap|
          close_at(eap) if eap.split(/[:_]/)[0] == host
        }
      end
      
      def close_at(ap)
        return unless @pool.key?(ap)
        @lock.synchronize {
          while(@pool[ap].length > 0)
            begin
              @pool[ap].shift[0].close
            rescue =>e
              Roma::Logging::RLogger.instance.error("#{__FILE__}:#{__LINE__}:#{e}")
            end
          end
          @pool.delete(ap)
        }
      end

    end # class ConPool

  end # module Messaging
end # module Roma
