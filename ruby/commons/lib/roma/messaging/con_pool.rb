require 'thread'
require 'socket'
require 'singleton'

module Roma
  module Messaging
    
    class ConPool
      include Singleton

      attr_accessor :maxlength

      def initialize(maxlength = 10)
        @pool = {}
        @maxlength = maxlength
        @lock = Mutex.new
      end

      def get_connection(ap)
        ret = @pool[ap].shift if @pool.key?(ap) && @pool[ap].length > 0
        ret = create_connection(ap) unless ret
      rescue
        nil
      end

      def return_connection(ap, con)
        if @pool.key?(ap) && @pool[ap].length > 0
          if @pool[ap].length > @maxlength
            con.close
          else
            @pool[ap] << con
          end
        else
          @pool[ap] = [con]
        end
      rescue
      end

      def create_connection(ap)
        addr, port = ap.split(/[:_]/)
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
              @pool[ap].shift.close
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
