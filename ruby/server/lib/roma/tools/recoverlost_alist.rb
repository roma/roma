#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
#
# usage:recoverlost_alist address port storage-path [yyyymmddhhmmss]
#
require 'roma/tools/recoverlost_lib'

Roma::RecoverLost.new('recoverlost_alist', 'alist_spushv', ARGV).suite
puts "Recover process has succeed."
