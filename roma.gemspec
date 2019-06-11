lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib)

require 'roma/version'

Gem::Specification.new do |s|
  s.authors = ["Junji Torii", "Hiroki Matsue", "Hiroaki Iwase"]
  s.homepage = 'http://roma-kvs.org/'
  s.name = "roma"
  s.version = Roma::VERSION
  s.license = 'GPL-3.0'
  s.summary = "ROMA server"
  s.description = <<-EOF
    ROMA is one of the data storing systems for distributed key-value stores. It is a completely decentralized distributed system that consists of multiple processes, called nodes, on several machines. It is based on pure P2P architecture like a distributed hash table, thus it provides high availability and scalability.
  EOF
  s.files = Dir[
    '[A-Z]*',
    'bin/**/*',
    'lib/**/*',
    'test/**/*.rb',
    'spec/**/*.rb',
    'doc/**/*',
    'examples/**/*',
  ]

  # Specify Ruby version of roma-1.1.0
  s.required_ruby_version = '>= 2.3.0'

  # Use these for libraries.
  s.require_path = 'lib'

  # Use these for applications.
  s.bindir = "bin"
  s.executables = Dir.entries('bin').reject{ |d| d =~ /^\.+$/ || d =~ /^sample_/ }

  s.default_executable = "romad"

  s.has_rdoc = true
  s.rdoc_options = [
                '--line-numbers',
                '--inline-source',
                "--main", "README.md",
                "-c UTF-8"
               ]
  s.extra_rdoc_files = ["README.md", "CHANGELOG.md"]

  s.add_dependency 'eventmachine', '~> 1.0.0'
  s.add_dependency 'jaro_winkler', '~> 1.3.5'
  s.add_dependency 'roma-storage', '~> 0.1.0'
  s.add_dependency 'thor', '~> 0.20.3'

  #s.add_development_dependency 'tokyocabinet', '~> 1.29.1' # we prepared customized gem(1.31a) for ruby2.1. Please use this.
  s.add_development_dependency 'ffi'
  s.add_development_dependency 'test-unit'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'roma-client'
end
