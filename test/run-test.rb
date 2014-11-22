#!/usr/bin/env ruby

require 'test/unit'
require 'pathname'

base_path = Pathname(__FILE__).dirname.parent.parent.expand_path
$LOAD_PATH.unshift("#{base_path}/server/lib")
$LOAD_PATH.unshift("#{base_path}/server/test")

client_base_path = Pathname(__FILE__).dirname.parent.parent.parent.parent.expand_path
$LOAD_PATH.unshift("#{client_base_path}/roma-ruby-client/lib")

require 'roma-test-utils'

Dir["#{base_path}/server/test/t_*.rb"].each do |test_file|
  require File.basename(test_file, '*.rb')
end

exit(Test::Unit::AutoRunner.run)
