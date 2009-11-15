#!/usr/bin/env ruby

require 'test/unit'
require 'pathname'

base_path = Pathname(__FILE__).dirname.parent.parent.expand_path
$LOAD_PATH.unshift("#{base_path}/server/lib")
$LOAD_PATH.unshift("#{base_path}/client/lib")
$LOAD_PATH.unshift("#{base_path}/commons/lib")
$LOAD_PATH.unshift("#{base_path}/server/test")

require 'roma-test-utils'

Dir["#{base_path}/server/test/t_*.rb"].each do |test_file|
  require File.basename(test_file, '*.rb')
end
