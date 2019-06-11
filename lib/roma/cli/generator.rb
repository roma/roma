require 'thor'

module Roma
  class CLI < Thor
    class Generator < Thor
      include Thor::Actions

      source_root File.expand_path('../', __dir__)

      desc 'config', 'Generate configuration file'
      def config(destination = './config.rb')
        copy_file('config.rb', destination)
      end
    end
  end
end
