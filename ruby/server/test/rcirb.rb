#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

require 'pp'
require 'irb'

path =  File.dirname(File.expand_path($PROGRAM_NAME))
$LOAD_PATH << path + "/../lib"
$LOAD_PATH << path  + "/../../commons/lib"
$LOAD_PATH << path  + "/../../client/lib"

require 'roma/client/rclient'

$rc=Roma::Client::RomaClient.new(['roma0_11211'])

IRB.start
