# 
# File: handler.rb
#
require 'eventmachine'
require 'roma/event/con_pool'
require 'roma/logging/rlogger'
require 'socket'

module Roma
  module Event

    class Handler < EventMachine::Connection
      @@ev_list={}
      def self.ev_list; @@ev_list; end

      @@ccl_start = 200
      @@ccl_rate = 30
      @@ccl_full = 300

      def self.get_ccl
        "#{@@ccl_start}:#{@@ccl_rate}:#{@@ccl_full}"
      end

      def self.set_ccl(ccl)
        if ccl =~ /^(\d+):(\d+):(\d+)$/
          s,r,f = $1.to_i,$2.to_i,$3.to_i
          return false if(s < 0 || f < 0 || r < 0 || r > 100 || s > f)
          @@ccl_start = s
          @@ccl_rate = r
          @@ccl_full = f
          return true
        else
          return false
        end
      end

      attr :stop_event_loop
      attr :connected
      attr :fiber
      attr :rbuf

      attr :storages
      attr :rttable
      attr_accessor :timeout
      attr_reader :lastcmd

      def initialize(storages, rttable)
        @rbuf=''
        unless has_event?
          public_methods.each{|m|
            if m.to_s.start_with?('ev_')
              add_event(m.to_s[3..-1],m)
            end
          }
        end
        @th1 = 100
        @close_rate = 70
        @th2 = 200

        @storages = storages
        @rttable = rttable
        @timeout = 10
        @log = Roma::Logging::RLogger.instance
      end

      def post_init
        @addr = Socket.unpack_sockaddr_in(get_peername)
        @log.info("Connected from #{@addr[1]}:#{@addr[0]}. I have #{EM.connection_count} connections.")

        @connected = true
        @fiber = Fiber.new { dispatcher }
      end

      def receive_data(data)
        @rbuf << data
        @fiber.resume
      rescue Exception =>e
        @log.error("#{__FILE__}:#{__LINE__}:#{@addr[1]}:#{@addr[0]} #{e.inspect} #{$@}")
      end

      def unbind
        @connected=false
        @fiber.resume
        EventMachine::stop_event_loop if @stop_event_loop
        @log.info("Disconnected from #{@addr[1]}:#{@addr[0]}")
      rescue Exception =>e
        @log.warn("#{__FILE__}:#{__LINE__}:#{@addr[1]}:#{@addr[0]} #{e.inspect} #{$@}")
      end

      protected

      def has_event?
        @@ev_list.length!=0
      end

      def add_event(c,m)
        @@ev_list[c]=m
      end

      def exit
        EventMachine::stop_event_loop
      end

      private

      def get_connection(ap)
        con=Roma::Event::EMConPool::instance.get_connection(ap)
        con.fiber=@fiber
        con
      end

      def return_connection(ap,con)
        Roma::Event::EMConPool.instance.return_connection(ap,con)
      end

      def dispatcher
        while(@connected) do
          next unless s=gets
          s=s.chomp.split(/ /)
          if s[0] && @@ev_list.key?(s[0].downcase)
            send(@@ev_list[s[0].downcase],s)
            @lastcmd=s
          elsif s.length==0
            next
          elsif s[0]=='!!'
            send(@@ev_list[@lastcmd[0].downcase],@lastcmd)
          else
            @log.warn("command error:#{s}")
            send_data("ERROR\r\n")
            close_connection_after_writing
          end

          d = EM.connection_count - @@ccl_start
          if d > 0 &&
              rand(100) < @@ccl_rate + (100 - @@ccl_rate) * d / (@@ccl_full - @@ccl_start)
            send_data("ERROR\r\n")
            close_connection_after_writing
            @log.warn("Connection count > #{@@ccl_start}:closed")
          end
        end
      rescue Exception =>e
        @log.warn("#{__FILE__}:#{__LINE__}:#{@addr[1]}:#{@addr[0]} #{e} #{$@}")
        close_connection
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

      def read_bytes(size, mult = 1)
        t=Time.now.to_i
        while(@connected) do
          d = pop(size)
          if d
            return d
          else
            remain = size - @rbuf.size
            Fiber.yield(remain)
            if Time.now.to_i - t > @timeout * mult
              @log.warn("#{__FILE__}:#{__LINE__}:#{@addr[1]}:#{@addr[0]} read_bytes time out");
              close_connection
              return nil
            end
          end
        end
        nil
      end

      def gets
        while(@connected) do
          if idx=@rbuf.index("\n")
            return pop(idx+1)
          else
            Fiber.yield(@rbuf.size)
          end
        end
        nil
      end

      def detach_socket
        @connected = false
        Socket::for_fd(detach)
      end

      def conn_get_stat
        ret = {}
        ret["connection.continuous_limit"] = Handler.get_ccl
        ret
      end

    end # class Handler < EventMachine::Connection

  end # module Event
end # module Roma
