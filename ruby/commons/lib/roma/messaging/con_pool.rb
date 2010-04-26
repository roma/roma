require 'thread'
require 'socket'
require 'singleton'

module Roma
  module Messaging
    
    class ConPool
      include Singleton

      attr_accessor :maxlength
      attr_accessor :refresh_rate

      def initialize(maxlength = 10, refresh_rate = 0.0001)
        @pool = {}
        @maxlength = maxlength
        @refresh_rate = refresh_rate
        @lock = Mutex.new
      end

      def get_connection(ap)
        ret = @pool[ap].shift if @pool.key?(ap) && @pool[ap].length > 0
        return create_connection(ap) unless ret
        ret
      rescue
        nil
      end

      def return_connection(ap, con)
        if rand < @refresh_rate
          con.close
          return
        end

        if select([con],nil,nil,0.0001)
          con.gets
          con.close
          return
        end

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
