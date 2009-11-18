#!/usr/bin/env ruby
#
# client proxy daemon
#
require 'optparse'
require 'eventmachine'
require 'timeout'
require 'singleton'
require 'roma/commons'
require 'roma/client/sender'
require 'roma/logging/rlogger'
require 'roma/client/client_rttable'

module Roma
  module Client
    module Proxy

      module RomaHandler
        attr_accessor :ap
        attr_reader :connected
    
        def post_init
          $log.info("Connected to roma")
          @connected = true
        end

        def unbind
          $log.info("Disconnected from roma")
          @connected = nil
        end
      end # module RomaHandler

      module ClientHandler

        def post_init
          $log.info("Connected from client")
          @cmd = ''
        end

        def receive_data(data)
          @cmd << data
          if @cmd.index("\n")
            @roma_h = get_roma_handler(@cmd)
            @roma_h.send_data(@cmd)
            @cmd = ''
            EM::enable_proxy(self, @roma_h)
            EM::enable_proxy(@roma_h, self)
          end
        rescue =>e
          $log.error("#{e} #{$@}")
        end

        def unbind
          $log.info("Disconnected from client")
          EM::disable_proxy(self)
          if @roma_h
            EM::disable_proxy(@roma_h)
            Conpool::instance.return_connection(@roma_h)
          end
        rescue =>e
          $log.error("#{e} #{$@}")
        end

        def get_roma_handler(cmd_line)
          cmd, key_hname = cmd_line.split(' ')
          key, hname = key_hname.split("\e")
          nid, d = Daemon::rttable.search_node(key)
          Conpool::instance.get_connection(nid, RomaHandler)
        rescue =>e
          $log.error("#{e} #{$@}")
        end

      end #  module ClientHandler

      class Conpool
        include Singleton

        attr_accessor :maxlength

        def initialize
          @pool = {}
          @maxlength = 10
          @lock = Mutex.new
        end

        def get_connection(ap, handler)
          ret = @pool[ap].shift if @pool.key?(ap) && @pool[ap].length > 0
          ret = create_connection(ap, handler) if ret == nil
          ret
        end

        def return_connection(con)
          return unless con.connected
          if @pool.key?(con.ap) && @pool[con.ap].length > 0
            if @pool[con.ap].length > @maxlength
              con.close_connection
            else
              @pool[con.ap] << con
            end
          else
            @pool[con.ap] = [con]
          end
        end

        def create_connection(ap, handler)
          addr,port = ap.split('_')
          con = EventMachine::connect(addr, port, handler)
          con.ap = ap
          con
        end
        
        def close_all
          @pool.each_key{|ap| close_at(ap) }
        end

        def close_at(ap)
          return unless @pool.key?(ap)
          @lock.synchronize {
            while(@pool[ap].length > 0)
              begin
                @pool[ap].shift.close_connection
              rescue =>e
                $log.error("#{e} #{$@}")
              end
            end
            @pool.delete(ap)
          }
        end
      end # class Conpool

      class Daemon
        attr_reader :daemon

        @@rttable = nil

        def self.rttable
          @@rttable
        end

        def initialize(argv = nil)
          options(argv)
          initialize_logger
          @sender = Roma::Client::Sender.new
          update_rttable(@init_nodes)
        end

        def initialize_logger
          Roma::Logging::RLogger.create_singleton_instance(@log_path,
                                                           @log_age,
                                                           @log_size)
        end

        def start
          
          timer
          
          loop do
            begin
              EventMachine::run do
                EventMachine.start_server('0.0.0.0', @port, ClientHandler)
                EventMachine.start_unix_domain_server("/tmp/#{@uds_name}", ClientHandler)
              end
            rescue =>e
              $log.error("#{e} #{$@}")
              retry
            end
          end
        end

        private

        def update_rttable(nodes)
          raise RuntimeError.new("nodes must not be nil.") unless nodes
          
          nodes.each { |node|
            rt = make_rttable(node)
            if rt != nil
              @@rttable = rt
              return
            end
          }
          raise RuntimeError.new("fatal error")
        end

        def make_rttable(node)
          mklhash = @sender.send_route_mklhash_command(node)
          return nil unless mklhash

          if @@rttable && @@rttable.mklhash == mklhash
            return @@rttable
          end

          rd = @sender.send_routedump_command(node)
          if rd
            ret = Roma::Client::ClientRoutingTable.new(rd)
            ret.mklhash = mklhash
            return ret
          end
          nil
        rescue =>e
          $log.error("#{e} #{$@}")
          nil
        end

        def options(argv)
          opts = OptionParser.new
          opts.banner="usage:#{File.basename($0)} [options] addr_port"

          @uds_name = 'roma'
          opts.on("-n", "--name [name]","Unix domain socket name.default=roma") { |v| @uds_name = v }
          @port = 12345
          opts.on("-p", "--port [port number]","default=12345"){ |v| @port = v.to_i }
          @log_path = "./rcdaemon.log"
          opts.on("-l", "--log [path]","default=./"){ |v|
            @log_path = v
            @log_path << "/" if @log_path[-1] != "/"
            @log_path << "rcdaemon.log"
          }

          @daemon = true
          opts.on(nil, "--debug"){ @daemon = false }
          @log_age = 10
          @log_size = 1024 * 1024

          opts.parse!(argv)
          raise OptionParser::ParseError.new if argv.length < 1
          @init_nodes = argv
        rescue OptionParser::ParseError => e
          $stderr.puts e.message
          $stderr.puts opts.help
          exit 1
        end

        def timer
          Thread.new do
            loop do
              sleep 10
              timer_event_10sec
            end
          end
        end

        def timer_event_10sec
          update_rttable(@@rttable.nodes)
        end

        def self.daemon
          p = Process.fork {
            pid=Process.setsid
            Signal.trap(:INT){
              exit! 0
            }
            Signal.trap(:TERM){
              exit! 0       
            }
            Signal.trap(:HUP){
              exit! 0
            }
            File.open("/dev/null","r+"){|f|
              STDIN.reopen f
              STDOUT.reopen f
              STDERR.reopen f
            }
            yield
          }
          $stderr.puts p
          exit! 0
        end

      end # class Daemon

    end # module Proxy
  end # module Client
end # module Roma

d = Roma::Client::Proxy::Daemon.new(ARGV)
$log = Roma::Logging::RLogger.instance
if d.daemon
  Roma::Client::Proxy::Daemon.daemon{ d.start }
else
  d.start
end
