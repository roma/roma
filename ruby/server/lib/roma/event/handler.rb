# 
# File: handler.rb
#
require 'eventmachine'
require 'roma/event/con_pool'
require 'roma/logging/rlogger'
require 'roma/stats'
require 'roma/storage/basic_storage'
require 'socket'

module Roma
  module Event

    class Handler < EventMachine::Connection
      @@ev_list={}
      def self.ev_list; @@ev_list; end
      @@system_commands={}
      def self.system_commands; @@system_commands; end

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

      @@connections = {}
      def self.connections; @@connections; end

      @@connection_expire_time = 60
      def self.connection_expire_time=(t)
        @@connection_expire_time = t
      end

      def self.connection_expire_time
        @@connection_expire_time
      end

      attr_accessor :timeout
      attr_reader :connected
      attr_reader :lastcmd
      attr_reader :last_access
      attr_reader :addr, :port

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
        @last_access = Time.now
      end

      def post_init
        @port, @addr = Socket.unpack_sockaddr_in(get_peername)
        @log.info("Connected from #{@addr}:#{@port}. I have #{EM.connection_count} connections.")
        @connected = true
        @last_access = Time.now
        @@connections[self] = @last_access
        @fiber = Fiber.new { dispatcher }
      rescue Exception =>e
        @log.error("#{__FILE__}:#{__LINE__}:#{e.inspect} #{$@}")
      end

      def receive_data(data)
        @rbuf << data
        @last_access = Time.now
        @fiber.resume
      rescue Exception =>e
        @log.error("#{__FILE__}:#{__LINE__}:#{@addr}:#{@port} #{e.inspect} #{$@}")
      end

      def unbind
        @log.debug("Roma::Event::Handler.unbind called")
        @connected=false
        begin
          @fiber.resume
        rescue FiberError
        end
        EventMachine::stop_event_loop if @stop_event_loop
        @@connections.delete(self)
        if @enter_time
          # hilatency check
          ps = Time.now - @enter_time
          if ps > @stats.hilatency_warn_time
            @log.warn("#{@lastcmd} has incompleted, passage of #{ps} seconds")
          end
        end
        @log.info("Disconnected from #{@addr}:#{@port}")
      rescue Exception =>e
        @log.warn("#{__FILE__}:#{__LINE__}:#{@addr}:#{@port} #{e.inspect} #{$@}")
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
        @stats = Roma::Stats.instance
        @log.debug("Roma::Event::Handler.dipatcher called")
        while(@connected) do
          @enter_time = nil
          next unless s=gets
          @enter_time = Time.now
          s=s.chomp.split(/ /)
          # check whether comand was send or not? and check this command listed on ROMA?
          if s[0] && @@ev_list.key?(s[0].downcase)
            send(@@ev_list[s[0].downcase],s)
            @lastcmd=s
            next if @@system_commands.key?(s[0].downcase)
          elsif s.length==0
            next
          elsif s[0]=='!!'
            send(@@ev_list[@lastcmd[0].downcase],@lastcmd)
            next if @@system_commands.key?(@lastcmd[0].downcase)
          else
            @log.warn("command error:#{s}")
            send_data("ERROR\r\n")
            close_connection_after_writing
            next
          end

          # hilatency check
          ps = Time.now - @enter_time
          if ps > @stats.hilatency_warn_time
            @log.warn("hilatency occurred in #{@lastcmd} put in a #{ps} seconds")
          end
          # check latency average
          #case @lastcmd[0]
          if @stats.latency_check_cmd.include?(@lastcmd[0])
            Roma::AsyncProcess::queue_latency.push(Roma::AsyncMessage.new('calc_latency_average', [ps, @lastcmd[0]]))
          end

          d = EM.connection_count - @@ccl_start
          if d > 0 &&
              rand(100) < @@ccl_rate + (100 - @@ccl_rate) * d / (@@ccl_full - @@ccl_start)
            send_data("ERROR\r\n")
            close_connection_after_writing
            @log.warn("Connection count > #{@@ccl_start}:closed")
          end
        end
      rescue Storage::StorageException => e
        @log.error("#{e.inspect} #{s} #{$@}")
        send_data("SERVER_ERROR #{e} in storage engine\r\n")
        close_connection_after_writing
        if Config.const_defined?(:STORAGE_EXCEPTION_ACTION) &&
            Config::STORAGE_EXCEPTION_ACTION == :shutdown
          @log.error("Romad will stop")
          @stop_event_loop = true
        end
      rescue Exception =>e
        @log.warn("#{__FILE__}:#{__LINE__}:#{@addr}:#{@port} #{e} #{$@}")
        close_connection
      end

      def pop(size)
        if size == 0
          return ''
        elsif size < 0
          return nil
        end

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
              @log.warn("#{__FILE__}:#{__LINE__}:#{@addr}:#{@port} read_bytes time out");
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
        ret["connection.count"] = EM.connection_count
        ret["connection.continuous_limit"] = Handler.get_ccl
        ret["connection.accepted_connection_expire_time"] = Handler.connection_expire_time
        ret["connection.handler_instance_count"] = Handler.connections.length
        ret["connection.pool_maxlength"] = Messaging::ConPool.instance.maxlength
        ret["connection.pool_expire_time"] = Messaging::ConPool.instance.expire_time
        ret["connection.EMpool_maxlength"] = Event::EMConPool::instance.maxlength
        ret["connection.EMpool_expire_time"] = Event::EMConPool.instance.expire_time
        ret
      end

    end # class Handler < EventMachine::Connection

  end # module Event
end # module Roma
