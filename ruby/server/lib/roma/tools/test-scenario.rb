#!/usr/bin/env ruby

require 'date'
require 'logger'
require 'roma/client/rclient'
require 'roma/tools/multi_commander'
require 'optparse'

module Roma
  module Test
    class RomaProc
      attr_accessor :addr
      attr_accessor :port
      attr_accessor :pid

      def initialize a, p
        @addr = a
        @port = p
      end

      def self.to_str procs
        msg = ""
        procs.each { |proc|
          msg = msg + proc.addr + "_" + proc.port.to_s + " "
        }
        msg
      end
    end

    class Stress
      attr :cnt
      attr :tmax
      attr :tmin
      attr :num_of_threads
      attr_accessor :runnable

      def initialize th_num
        @cnt = 0
        @tmax = 0
        @tmin = 100
        @num_of_threads = th_num
        @runnable = true
      end

      def start addr, port
        Thread.new {
          sleep_time=10
          while @runnable
            sleep sleep_time
            printf("qps=%d max=%f min=%f ave=%f\n", @cnt / sleep_time, @tmax, @tmin, sleep_time / @cnt.to_f)
            @@cnt=0
            @@tmax=0
            @@tmin=100
          end
        }

        working_threads = []
        @num_of_threads.times {
          working_threads << Thread.new {
            send_random_reqs addr, port
          }
        }
      end

      def send_random_reqs addr, port
        rc = Roma::Client::RomaClient.new([ "#{addr}_#{port.to_s}" ])
        n=1000
        while @runnable
          begin 
            i = rand(n)
            ts = DateTime.now
            case rand(3)
            when 0
              res = rc.set(i.to_s, 'hoge' + i.to_s)
              puts "set k=#{i} #{res}" if res==nil || res.chomp != 'STORED'
            when 1
              res = rc.get(i.to_s)
              puts "get k=#{i} #{res}" if res == :error
            when 2
              res = rc.delete(i.to_s)
              puts "del k=#{i} #{res}" if res != 'DELETED' && res != 'NOT_FOUND'
            end
            t = (DateTime.now - ts).to_f * 86400.0
            @tmax=t if t > @tmax
            @tmin=t if t < @tmin
            @cnt+=1
          rescue => e
            p e
          end
        end
      rescue => e
        p e
      end
      private :send_random_reqs
    end

    class Scenario
      attr :working_path
      attr :roma_procs
      attr :stress
      attr :log

      def initialize(path, procs)
        @working_path = path
        @roma_procs = procs
        @stress = Stress.new 1
        @log = Logger.new "./test-scenario.log", "daily"
      end

      def init_roma
        @log.debug "begin init_roma"
        exec "rm -f localhost_1121?.*"
        exec "bin/mkroute -d 7 #{RomaProc.to_str(@roma_procs)} --enabled_repeathost"
        @log.debug "end init_roma"
      end

      def start_roma
        @log.debug "begin start_roma"
        @roma_procs.length.times { |i|
          start_roma_proc i
        }
        @log.debug "end start_roma"
      end

      def start_roma_proc i
        @log.debug "begin start_roma_proc"
        str = "bin/romad #{@roma_procs[i].addr} -p #{@roma_procs[i].port.to_s} -d --enabled_repeathost"
        exec str
        @roma_procs[i].pid = get_pid(str)
        @log.debug "end start_roma_proc"
      end
      private :start_roma_proc

      def exec cmd
        `cd #{@working_path}; #{cmd}`
      end
      private :exec

      def get_pid reg_str
        open("| ps -ef | grep romad") { |f|
          while l = f.gets
            return $1.to_i if l =~ /(\d+).+ruby\s#{reg_str}/
          end
        }
        nil
      end
      private :get_pid

      def stop_roma
        @log.debug "begin start_roma"
        @roma_procs.length.times { |i|
          stop_roma_proc i
        }
        @log.debug "end start_roma"
      end

      def stop_roma_proc i
        @log.debug "begin start_roma_proc"
        exec "kill -9 #{@roma_procs[i].pid}"
        @log.debug "end start_roma_proc"
      end
      private :stop_roma_proc

      def start_roma_client addr, port
        @stress.start addr, port
      end

      def stop_roma_client
        @stress.runnable = false
      end

      def send_recover addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "recover", "#{addr}_#{port}"
        puts res
      end

      def send_stats addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "stats run", "#{addr}_#{port}"
        puts res
      end

      def send_stats_routing_nodes_length addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "stats routing.nodes.length", "#{addr}_#{port}"
        splited = res.split(' ')
        splited.each_with_index { |w, i|
          if w == "routing.nodes.length"
            return splited[i + 1].to_i
          end
        }
        raise "not found a specified property: routing.nodes.length"
      end
      
      def send_stats_run_acquire_vnodes addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "stats stats.run_acquire_vnodes", "#{addr}_#{port}"
        splited = res.split(' ')
        splited.each_with_index { |w, i|
          if w == "stats.run_acquire_vnodes"
            return splited[i + 1] == "true"
          end
        }
        raise "not found a specified property: stats.run_acquire_vnodes"
      end

      def test_kill_join_recover
        @log.info "begin method test_kill_join_recover"

        # initialize a ROMA
        init_roma

        # start a ROMA
        start_roma

        sleep 10

        # stress 
        start_roma_client @roma_procs[0].addr, @roma_procs[0].port

        sleep 2

        nlen = send_stats_routing_nodes_length @roma_procs[0].addr, @roma_procs[0].port
        if nlen != 3
          raise "fatal error nlen: #{nlen}"
        end

        sleep 2

        # stop the specified roma process
        stop_roma_proc 2

        sleep 10

        nlen = send_stats_routing_nodes_length @roma_procs[0].addr, @roma_procs[0].port
        if nlen != 2
          raise "fatal error nlen: #{nlen}"
        end

        #ret = send_stats_run_acquire_vnodes @roma_procs[0].addr, @roma_procs[0].port
        #puts "$$ #{ret}"
        #send_stats @roma_procs[0].addr, @roma_procs[0].port
        #puts "$$"
        #ret = send_stats_run_acquire_vnodes @roma_procs[0].addr, @roma_procs[0].port
        #puts "$$ #{ret}"
        #send_stats @roma_procs[0].addr, @roma_procs[0].port
        #send_recover @roma_procs[0].addr, @roma_procs[0].port


        sleep 2

        stop_roma_client

        #stop_roma
        stop_roma_proc 0
        stop_roma_proc 1

        @log.info "end method test_kill_join_recover"
      end

      def test_suite
        test_kill_join_recover
      end
    end

    class Config
      attr_reader :number_of_nodes
      attr_reader :port
      attr_reader :hostname
      attr_reader :working_path

      def initialize(argv)
        opts = OptionParser.new
        opts.banner="usage:#{File.basename($0)} [options]"
        
        opts.on_tail("-h", "--help", "show this message") {
          puts opts; exit
        }
        @number_of_nodes = 3
        opts.on("-n N", "number of nodes[default: 3]", Integer) { |v|
          @number_of_nodes = v
        }

        @working_path = '.'
        opts.on("-p PATH", "working path[default: .]", String) { |v|
          @working_path = v
        }

        @hostname = 'localhost'
        opts.on("--hname HOSTNAME", "hostname[default: localhost]", String) { |v|
          @hostname = v
        }

        @port = 11211
        opts.on("--port PORT_NUMBER", "port number[default: 11211]", Integer) { |v|
          @port = v
        }

        opts.parse!(argv)
      rescue OptionParser::ParseError => e
        $stderr.puts e.message
        $stderr.puts opts.help
        exit 1
      end
    end

  end
end

cnf = Roma::Test::Config.new(ARGV)

# check for a working path
unless File::exist?("#{cnf.working_path}/bin/romad")
  # in invalid path
  $stderr.puts "#{cnf.working_path}/bin/romad dose't found"
  $stderr.puts "You should set to a working path option(-p)."
  exit 1
end

procs = []
cnf.number_of_nodes.times{ |i|
  procs << Roma::Test::RomaProc.new(cnf.hostname, cnf.port + i)  
}

s = Roma::Test::Scenario.new(cnf.working_path, procs)
s.test_suite
