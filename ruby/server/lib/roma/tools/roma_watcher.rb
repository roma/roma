#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

require 'kconv'
require 'logger'
require 'socket'
require 'timeout'
require 'yaml'

module Roma
  module Watch
    module Message
      ERROR_NODE_DOWN = 'A node down'
      ERROR_SPLIT_BRAIN = 'Split brain'
      COMMAND_NODELIST = 'nodelist'
      COMMAND_QUIT = 'quit'
    end

    class Mailer
      MAILER = '/usr/lib/sendmail'

      attr :from
      attr :to
      attr :mailer

      def initialize(from, to, mailer = nil)
        @from = from
        @to = to
        @mailer = mailer
        @mailer ||= MAILER
      end

      def send_mail(sub, msg)
        open("| #{@mailer} -f #{@from} -t", 'w') do |f|
          f.puts "From: #{@from}"
          f.puts "To: #{@to}"
          #f.puts "Subject: #{sub.tojis}"
          f.puts "Subject: #{sub}"
          f.puts "Reply-To: #{@from}"
          f.puts
          f.puts msg.tojis
          2.times{ f.puts }
          f.puts "."
        end
      end
    end # Mailer

    class Main
      attr :conf
      attr :log
      attr :nodelist_inf
      attr :errors
      attr :mailer

      def initialize config
        @conf = config
        @log = Logger.new @conf['log']['path'], @conf['log']['rotate']
        @nodelist_inf = {}
        @errors = {}
        @subject_prefix = @conf['mail']['subject_prefix']
        @mailer = Mailer.new @conf['mail']['from'], @conf['mail']['to'], @conf['mail']['mailer']
      end

      def watch
        @log.info "start watching a ROMA"
        watch_nodes
        @log.info "end watching"
        @log.info "start checking a ROMA"
        check_nodes
        @log.info "end checking"
      end

      def watch_nodes
        @conf['roma'].each { |node|
          nodes = watch_node node
          @nodelist_inf[node] = nodes if nodes
        }
      end

      def watch_node node
        @log.debug "start watching a node: #{node}"
        host, port = node.split(':')
        sock = nil
        begin
          timeout(@conf['timeout'].to_i) {
            line = nil
            TCPSocket.open(host, port) do |sock|
              sock.puts Message::COMMAND_NODELIST
              line = sock.gets.chomp!
              sock.puts Message::COMMAND_QUIT
            end
            @log.debug "end watching a node: #{node}"
            line.split(' ')
          }
        rescue Exception => e
          emsg = "Catch an error when checking a node #{node}: #{e.to_s}"
          @log.error emsg
          if (cnt ||= 0; cnt += 1) < @conf['retry']['count'].to_i
            @log.info "retry: #{cnt} times"
            sleep @conf['retry']['period'].to_i
            retry
          end
          @errors[node] = emsg
          nil
        end
      end

      def check_nodes
        check_vital
        check_splitbrain
      end

      def check_vital
        @log.debug "start checking the vital"
        @errors.each { |node, emsg|
          @mailer.send_mail(@subject_prefix + Message::ERROR_NODE_DOWN, emsg)
        }
        @log.debug "end checking the vital"
      end

      def check_splitbrain
        @log.debug "start checking a splitbrain"
        all_ring = []
        @nodelist_inf.each { |node, ring|
          all_ring << ring unless all_ring.include? ring
        }

        if all_ring.size != 1
          emsg = ""
          all_ring.each { |ring|
            emsg += "#{ring.join(',')}\r\n"
          }
          @mailer.send_mail(@subject_prefix + Message::ERROR_SPLIT_BRAIN, emsg)
        end
        @log.debug "end checking a splitbrain"
      end
    end
  end # Watch
end # Roma

def usage
  puts File.basename(__FILE__) + " config.yml"
end

if 1 == ARGV.length
  config = YAML.load_file(ARGV[0])
  Roma::Watch::Main.new(config).watch
else
  usage
end
