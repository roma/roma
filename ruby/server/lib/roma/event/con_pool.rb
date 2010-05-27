#
# File: con_pool.rb
#
require 'singleton'
require 'eventmachine'
require 'roma/logging/rlogger'

module Roma
  module Event
    module EMConnection
      attr_writer :fiber
      attr_reader :connected
      attr_accessor :ap

      def post_init
        @rbuf = ''
        @connected = true
      end
      
      def receive_data(data)
        @rbuf << data
        @fiber.resume
      rescue =>e
        Roma::Logging::RLogger.instance.error("#{__FILE__}:#{__LINE__}:#{e.inspect} #{$@}")
      end

      def unbind
        @connected = nil
        @fiber.resume
      rescue FiberError
      rescue =>e
        Roma::Logging::RLogger.instance.warn("#{__FILE__}:#{__LINE__}:#{e.inspect} #{$@}")
      end

      def send(data)
        send_data(data)
      end

      def pop(size)
        if @rbuf.size >= size
          r = @rbuf[0..size-1]
          @rbuf = @rbuf[size..-1]
          r
        else
          nil
        end
      end

      def read_bytes(size)
        while(@connected) do
          d = pop(size)
          if d
            return d
          else
            remain = size - @rbuf.size
            Fiber.yield(remain)
          end
        end
        nil
      end

      def gets
        while(@connected) do
          if idx = @rbuf.index("\n")
            return pop(idx+1)
          else
            Fiber.yield(@rbuf.size)
          end
        end
        nil
      end

    end # module EMConnection

    class EMConPool
      include Singleton
      attr :pool
      attr_accessor :maxlength
      attr_accessor :refresh_rate

      def initialize
        @pool = {}
        @maxlength = 10
        @refresh_rate = 0.0001
        @lock = Mutex.new
      end

      def get_connection(ap)
        ret = @pool[ap].shift if @pool.key?(ap) && @pool[ap].length > 0
        ret = create_connection(ap) if ret == nil || ret.connected != true
        ret
      end

      def return_connection(ap, con)
        return if con.connected == false

        if rand < @refresh_rate
          con.close_connection
          return
        end

        if @pool.key?(ap) && @pool[ap].length > 0
          if @pool[ap].length > @maxlength
            con.close_connection
          else
            @pool[ap] << con
          end
        else
          @pool[ap] = [con]
        end
      end

      def create_connection(ap)
        addr,port = ap.split(/[:_]/)
        con = EventMachine::connect(addr, port, Roma::Event::EMConnection)
        con.ap = ap
        con
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
              @pool[ap].shift.close_connection
            rescue =>e
              Roma::Logging::RLogger.instance.error("#{__FILE__}:#{__LINE__}:#{e.inspect}")
            end
          end
          @pool.delete(ap)
        }
      end

    end # class EMConPool

  end # module Event
end # module Roma
