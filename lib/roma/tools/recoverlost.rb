#!/usr/bin/env ruby
#
# usage:recoverlost address port storage-path [yyyymmddhhmmss]
#
require 'roma/tools/recoverlost_lib'

Roma::RecoverLost.new('recoverlost', 'spushv', ARGV).suite
puts "Recover process has succeed."
