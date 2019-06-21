require 'thor'

module Roma
  class CLI < Thor
    class Config < Thor
      include Thor::Actions

      source_root File.expand_path('../', __dir__)

      desc 'create', 'Create configuration file'
      def create(destination = './config.rb')
        copy_file('config.rb', destination)
      end
    end
  end
end
