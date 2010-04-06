require 'roma/stats'
require 'roma/version'
require 'roma/event/handler'
require 'roma/messaging/con_pool'
require 'roma/command/bg_command_receiver'
require 'roma/command/rt_command_receiver'
require 'roma/command/st_command_receiver'
require 'roma/command/util_command_receiver'
require 'roma/command/mh_command_receiver'
require 'roma/command/sys_command_receiver'

module Roma
  module Command

    class Receiver < Roma::Event::Handler

      include BackgroundCommandReceiver
      include RoutingCommandReceiver
      include StorageCommandReceiver
      include UtilCommandReceiver
      include MultiHashCommandReceiver
      include SystemCommandReceiver

      def initialize(storages, rttable)
        super(storages, rttable)
        @stats = Roma::Stats.instance
        @nid = @stats.ap_str
        @defhash = 'roma'
      end

      def self.mk_starting_evlist
        Event::Handler.ev_list.clear
        methods = RoutingCommandReceiver::public_instance_methods
        methods << BackgroundCommandReceiver::public_instance_methods
        methods << SystemCommandReceiver::public_instance_methods

        if Receiver::public_instance_methods.include?(:ev_eval)
          methods << :ev_eval
        end

        methods.flatten.each{|m|
          if m.to_s.start_with?('ev_')
            Event::Handler.ev_list[m.to_s[3..-1]] = m
          end
        }
      end

      def self.mk_evlist
        Receiver::public_instance_methods.each{|m|
          if m.to_s.start_with?('ev_')
            Event::Handler.ev_list[m.to_s[3..-1]] = m
          end
        }
      end

    end # class Receiver < Roma::Event::Handler

  end # module Command
end # module Roma
