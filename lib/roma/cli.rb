require 'thor'
require 'roma/cli/config'
require 'roma/cli/server'
require 'roma/cli/routing'

module Roma
  class CLI < Thor
    register(CLI::Config, 'config', '', '')
    register(CLI::Server, 'server', '', '')
    register(CLI::Routing, 'routing', '', '')

    map '-V' => 'version'

    desc 'version', "Show roma server's version"
    def version
      puts Roma::VERSION
    end
  end
end