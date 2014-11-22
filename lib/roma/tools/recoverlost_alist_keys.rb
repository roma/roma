#!/usr/bin/env ruby
#
# usage:recoverlost_alist_keys address port storage-path key-list
#
require 'roma/tools/recoverlost_lib'

r = Roma::RecoverLost.new('recoverlost_alist_keys', 'alist_spushv', ARGV, true)

keys = []
while(key = STDIN.gets)
  keys << key.chomp
end

r.suite_with_keys(keys)

puts "Recover process has succeed."
