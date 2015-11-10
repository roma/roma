#!/usr/bin/env ruby

require 'socket'

class DynamicCmdTest < Test::Unit::TestCase
  include RomaTestUtils

  def setup
    start_roma
    @sock = TCPSocket.new("localhost", 11211)
  end

  def teardown
    @sock.close if @sock
    stop_roma
  rescue => e
    puts "#{e} #{$@}"
  end

  def test_jaro_winkler
    @sock.write("staat\r\n")
    assert_equal("", @sock.gets.chomp)
    assert_equal("ERROR: 'staat' is not roma command.", @sock.gets.chomp)
    assert_equal("Did you mean this?", @sock.gets.chomp)
    assert_equal("\tstat", @sock.gets.chomp)

    @sock.write("outingdump yaml\r\n")
    assert_equal("", @sock.gets.chomp)
    assert_equal("ERROR: 'outingdump' is not roma command.", @sock.gets.chomp)
    assert_equal("Did you mean this?", @sock.gets.chomp)
    assert_equal("\troutingdump", @sock.gets.chomp)

    @sock.write("hoge\r\n")
    assert_equal("ERROR: 'hoge' is not roma command. Please check command.", @sock.gets.chomp)
    assert_equal("(closing telnet connection command is 'quit')", @sock.gets.chomp)
  end


end
