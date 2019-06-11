require 'thor'
require 'roma/config'
require 'roma/server'

module Roma
  class CLI < Thor
    class Server < Thor
      class_option :verbose, type: :boolean, default: false, aliases: '-v', desc: 'Show logs verbosely'

      method_option :daemon, type: :boolean, default: false, aliases: '-d', desc: 'Run as a daemon'
      method_option :join, type: :string, aliases: '-j', desc: 'Concatination of server address and port with ":"'
      method_option :name, type: :string, aliases: '-n', default: Config::DEFAULT_NAME, desc: "Server's name"
      method_option :port, type: :numeric, default: Config::DEFAULT_PORT, aliases: '-p', desc: 'Port number'
      method_option :enabled_repeathost, type: :boolean, default: false, desc: 'Allow redundancy to same host'
      method_option :disabled_cmd_protect, type: :boolean, default: false, desc: 'Command protection disable while starting'
      method_option :config, type: :string, default: nil, aliases: '-c', desc: 'File path to configuration'
      desc 'start [OPTIONS] ADDRESS', 'Launch ROMA server'
      def start(address)
        server = Roma::Server.new(options)
        server.start
      end
    end
  end
end