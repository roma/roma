# -*- coding: utf-8 -*-
require 'rubygems'
require 'rake'
require 'rake/gempackagetask'
require 'rake/rdoctask'

RDOC_OPTIONS = [
                '--line-numbers',
                '--inline-source',
                "--main", "README.rdoc",
                "-c UTF-8",
               ]

# gem tasks
PKG_FILES = FileList[
  '[A-Z]*',
  'bin/**/*',
  'lib/**/*.rb',
  'test/**/*.rb',
  'spec/**/*.rb',
  'doc/**/*',
  'examples/**/*',
]

VER_NUM = `ruby -Ilib -e 'require "roma/client/version"; puts Roma::Client::VERSION::STRING'`

if VER_NUM =~ /([0-9.]+)$/
  CURRENT_VERSION = $1
else
  CURRENT_VERSION = "0.0.0"
end

SPEC = Gem::Specification.new do |s|
  s.name = "roma-client"
  s.version = CURRENT_VERSION
  s.summary = "ROMA client library"
  s.description = <<-EOF
    ROMA client library
  EOF
  s.files = PKG_FILES.to_a

  s.require_path = 'lib'                         # Use these for libraries.

  s.has_rdoc = true
  s.rdoc_options.concat RDOC_OPTIONS
  s.extra_rdoc_files = ["README.rdoc"]

  s.add_dependency('roma-commons')
end

package_task = Rake::GemPackageTask.new(SPEC) do |pkg|
end


Rake::RDocTask.new("doc") { |rdoc|
  rdoc.rdoc_dir = 'doc'
  rdoc.title = "ROMA documents"
  rdoc.options.concat RDOC_OPTIONS
  rdoc.rdoc_files.include('lib/**/*.rb')
  rdoc.rdoc_files.include("README.rdoc")
}
