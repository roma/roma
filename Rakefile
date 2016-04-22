require 'bundler/gem_tasks'
require 'rake'

begin
  require 'rdoc/task'
rescue LoadError
  require 'rake/rdoctask'
end

RDOC_OPTIONS = [
                '--line-numbers',
                '--inline-source',
                "--main", "README.md",
                "-c UTF-8",
               ]

Rake::RDocTask.new("doc") do |rdoc|
  rdoc.rdoc_dir = 'doc'
  rdoc.title = "ROMA documents"
  rdoc.options.concat RDOC_OPTIONS
  rdoc.rdoc_files.include('lib/**/*.rb')
  rdoc.rdoc_files.include("README.md")
  rdoc.rdoc_files.include("ChangeLog.md")
end
