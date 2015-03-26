lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib)

require 'rake'
require 'roma/version'

Gem::Specification.new do |s|
  s.authors = ["Junji Torii", "Hiroki Matsue", "Hiroaki Iwase"]
  s.homepage = 'http://roma-kvs.org/'
  s.name = "roma"
  s.version = Roma::VERSION
  s.license = 'GPL-3.0'
  s.summary = "ROMA server"
  s.description = <<-EOF
    ROMA server
  EOF
  s.files = FileList[
    '[A-Z]*',
    'bin/**/*',
    'lib/**/*',
    'test/**/*.rb',
    'spec/**/*.rb',
    'doc/**/*',
    'examples/**/*',
  ]

  # Specify Ruby version roma-1.1.0 must support
  s.required_ruby_version = '>= 2.1.0'

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
                "--main", "README",
                "-c UTF-8"
               ]
  s.extra_rdoc_files = ["README", "CHANGELOG"]

  # TODO: for each gem, which version does rom depend on?
  s.add_dependency 'eventmachine'

  s.add_development_dependency 'ffi'
  s.add_development_dependency 'gdbm'
  s.add_development_dependency 'sqlite3'
  s.add_development_dependency 'rroonga'
  s.add_development_dependency 'test-unit'
  s.add_development_dependency 'rake'
end
