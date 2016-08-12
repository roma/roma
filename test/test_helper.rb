require 'test/unit'
require 'pathname'

base_path = Pathname(__FILE__).dirname.parent.expand_path

client_base_path = Pathname(__FILE__).dirname.parent.parent.expand_path
$LOAD_PATH.unshift("#{client_base_path}/roma-ruby-client/lib")

require 'roma-test-utils'

puts "* ROMA Version : #{Roma::Config::VERSION}"
puts "* Ruby Client Version : #{Roma::Client::VERSION::STRING}"
