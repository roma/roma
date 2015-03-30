#!/usr/bin/env ruby
#
# usage:recoverlost_alist address port storage-path
#
require 'roma/tools/recoverlost_lib'

Roma::RecoverLost.new('recoverlost_alist_all', 'alist_spushv', ARGV, true).suite
puts "Recover process has succeed."
