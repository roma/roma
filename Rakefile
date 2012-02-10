# -*- coding: utf-8 -*-
require 'rubygems'
require 'rake'

begin
  require 'rubygems/package_task'
  PackageTask = Gem::PackageTask
rescue LoadError
  require 'rake/gempackagetask'
  PackageTask = Rake::GemPackageTask
end

begin
  require 'rdoc/task'
rescue LoadError
  require 'rake/rdoctask'
end

RDOC_OPTIONS = [
                '--line-numbers',
                '--inline-source',
                "--main", "README",
                "-c UTF-8",
               ]

# gem tasks
base = 'ruby/server/'
PKG_FILES = FileList[
  '[A-Z]*',
  base + 'bin/**/*',
  base + 'lib/**/*',
  base + 'test/**/*.rb',
  base + 'spec/**/*.rb',
  base + 'doc/**/*',
  base + 'examples/**/*',
]

EXEC_TABLE = Dir.entries(base + 'bin').reject{ |d| d =~ /^\.+$/ || d =~ /^sample_/ }

require File.expand_path(File.join('ruby', 'server', 'lib', 'roma', 'version'), File.dirname(__FILE__))
VER_NUM = Roma::VERSION

if VER_NUM =~ /([0-9.]+)$/
  CURRENT_VERSION = $1
else
  CURRENT_VERSION = "0.0.0"
end

SPEC = Gem::Specification.new do |s|
  s.authors = ["Junji Torii", "Hiroki Matsue"]
  s.homepage = 'http://code.google.com/p/roma-prj/'
  s.name = "roma"
  s.version = CURRENT_VERSION
  s.summary = "ROMA server"
  s.description = <<-EOF
    ROMA server
  EOF
  s.files = PKG_FILES.to_a

  # Use these for libraries.
  s.require_path = base + 'lib'

  # Use these for applications.
  s.bindir = base + "bin"
  s.executables = EXEC_TABLE
  s.default_executable = "romad"

  s.has_rdoc = true
  s.rdoc_options.concat RDOC_OPTIONS
  s.extra_rdoc_files = ["README", "CHANGELOG"]

  s.add_dependency('eventmachine')
end

package_task = PackageTask.new(SPEC) do |pkg|
end


Rake::RDocTask.new("doc") { |rdoc|
  rdoc.rdoc_dir = 'doc'
  rdoc.title = "ROMA documents"
  rdoc.options.concat RDOC_OPTIONS
  rdoc.rdoc_files.include('lib/**/*.rb')
  rdoc.rdoc_files.include("README")
}
