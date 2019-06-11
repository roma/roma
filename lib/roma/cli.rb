require 'thor'
require 'roma/cli/server'

module Roma
  class CLI < Thor
    register(CLI::Server, 'server', '', '')

    map '-V' => 'version'

    desc 'version', "Show roma server's version"
    def version
      puts Roma::VERSION
    end
  end
end