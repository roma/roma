#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
#
# usage:recoverlost address port storage-path [yyyymmddhhmmss]
#
require 'roma/tools/recoverlost_lib'

Roma::RecoverLost.new('recoverlost', 'spushv', ARGV).suite
puts "Recover process has succeed."
