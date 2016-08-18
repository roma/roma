require 'bundler/gem_tasks'
require 'rake'
require 'rake/testtask'

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
  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "ROMA documents"
  rdoc.options.concat RDOC_OPTIONS
  rdoc.rdoc_files.include('lib/**/*.rb')
  rdoc.rdoc_files.include("README.md")
  rdoc.rdoc_files.include("ChangeLog.md")
end

Rake::TestTask.new do |t|
  t.libs << 'test'
  t.test_files = FileList['test/**/test_*.rb']
  t.verbose = true
  t.options = '--verbose'
end

namespace :changelog do
  task :update do
    last_released_tag = `git tag -l --sort=-creatordate | head -n1`.chomp
    prs = `git log --oneline #{last_released_tag}...master | grep --color=never -G -e 'Merge pull request' | awk '{gsub("#",""); print $5}'`.chomp

    require 'net/http'
    require 'json'
    http = Net::HTTP.new('api.github.com', 443)
    http.use_ssl = true

    prs.each_line do |pr|
      req = Net::HTTP::Get.new("/repos/roma/roma/pulls/#{pr.chomp}")
      res = http.request(req)

      if Net::HTTPSuccess === res
        json = JSON.parse(res.body)
        puts "* #{json['title']}, [#{json['user']['login']}](#{json['user']['html_url']}), [##{json['number']}](#{json['html_url']})"
      end
    end
  end
end

task default: :test
