require 'thor'
require 'roma/cli/server'
require 'roma/cli/generator'

module Roma
  class CLI < Thor
    register(CLI::Server, 'server', '', '')
    register(CLI::Generator, 'generate', '', '')

    map '-V' => 'version'

    desc 'version', "Show roma server's version"
    def version
      puts Roma::VERSION
    end
  end
end