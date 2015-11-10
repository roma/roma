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

  def test_stat_log_level
    @sock.write("stat log_level\r\n")
    assert_equal("stats.log_level debug", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)
  end

  def test_stat_secondary
    @sock.write("stat secondary\r\n")
    column, param = @sock.gets.chomp.split("\s")
    assert_equal("routing.secondary1", column)
    assert_equal(true, param.to_i > 1)
    assert_equal("END", @sock.gets.chomp)
  end

  def test_shutdown_self_no
    @sock.write("shutdown_self\r\n")
    assert_equal("", @sock.gets.chomp)
    assert_equal("=================================================================", @sock.gets.chomp)
    assert_equal("CAUTION!!: ", @sock.gets.chomp)
    assert_equal("\tThis command kill the instance!", @sock.gets.chomp)
    assert_equal("\tThere is some possibility of occuring redundancy down!", @sock.gets.chomp)
    assert_equal("=================================================================", @sock.gets.chomp)
    assert_equal("", @sock.gets.chomp)
    assert_equal("Are you sure to shutdown this instance?(yes/no)", @sock.gets.chomp)

    @sock.write("no\r\n")
    assert_nil(@sock.gets)
  end

  def test_shutdown_self_yes
    @sock.write("shutdown_self\r\n")
    assert_equal("", @sock.gets.chomp)
    assert_equal("=================================================================", @sock.gets.chomp)
    assert_equal("CAUTION!!: ", @sock.gets.chomp)
    assert_equal("\tThis command kill the instance!", @sock.gets.chomp)
    assert_equal("\tThere is some possibility of occuring redundancy down!", @sock.gets.chomp)
    assert_equal("=================================================================", @sock.gets.chomp)
    assert_equal("", @sock.gets.chomp)
    assert_equal("Are you sure to shutdown this instance?(yes/no)", @sock.gets.chomp)

    @sock.write("yes\r\n")
    assert_equal("BYE", @sock.gets.chomp)
    assert_nil(@sock.gets)

    sock2 = TCPSocket.new("localhost", 11212)
    sock2.write("nodelist\r\n")
    nodelist = sock2.gets.chomp.split("\s")
    assert_equal(1, nodelist.size)
    assert_equal("localhost_11212", nodelist[0])
  end

end
