#!/usr/bin/env ruby

require 'socket'
require 'test/unit'

class  LogShiftTest  < Test::Unit::TestCase
  self.test_order = :defined
  include RomaTestUtils

  def setup
    start_roma
    @sock_port_1 = TCPSocket.new("localhost", 11211)
    @sock_port_2 = TCPSocket.new("localhost", 11212)
  end

  def teardown
    @sock_port_1.close if @sock_port_1
    @sock_port_2.close if @sock_port_2
    stop_roma
    rescue => e
    puts "#{e} #{$@}"
  end

  def test_log_shift_size
    # get Current log_shift_size and log_shift_age of port 11211
    @sock_port_1.write("stat log_shift_size\r\n")
    old_log_shift_size = @sock_port_1.gets.chomp

    # set & check new log_shift_size value on port 11211
    @sock_port_1.write("set_log_shift_size 4096\r\n")
    assert_equal("END", @sock_port_1.gets.chomp)
    assert_equal('{"localhost_11212"=>"STORED", "localhost_11211"=>"STORED"}', @sock_port_1.gets.chomp)
    @sock_port_1.write("stat log_shift_size\r\n")
    assert_equal( "stats.log_shift_size 4096" , @sock_port_1.gets.chomp);
    assert_equal("END", @sock_port_1.gets.chomp)

    # get & check new log_shift_size value on port 11212
    @sock_port_2.write("stat log_shift_size\r\n")
    assert_equal( "stats.log_shift_size 4096" , @sock_port_2.gets.chomp);
    assert_equal("END", @sock_port_2.gets.chomp)

  end

    def test_log_shift_age
    # get Current log_shift_size and log_shift_age of port 11211
    @sock_port_1.write("stat log_shift_age\r\n")
    old_log_shift_age = @sock_port_1.gets.chomp

    # set & check new log_shift_size value on port 11211
    @sock_port_1.write("set_log_shift_age 7\r\n")
    assert_equal("END", @sock_port_1.gets.chomp)
    assert_equal('{"localhost_11212"=>"STORED", "localhost_11211"=>"STORED"}', @sock_port_1.gets.chomp)
    @sock_port_1.write("stat log_shift_age\r\n")
    assert_equal( "stats.log_shift_age 7" , @sock_port_1.gets.chomp);
    assert_equal("END", @sock_port_1.gets.chomp)

    # get & check new log_shift_size value on port 11212
    @sock_port_2.write("stat log_shift_age\r\n")
    assert_equal( "stats.log_shift_age 7" , @sock_port_2.gets.chomp);
    assert_equal("END", @sock_port_2.gets.chomp)

  end

  def test_log_shift_age_min
    # get Current log_shift_size and log_shift_age of port 11211
    @sock_port_1.write("stat log_shift_age\r\n")
    old_log_shift_age = @sock_port_1.gets.chomp

    # set & check new log_shift_size value on port 11211
    @sock_port_1.write("set_log_shift_age min\r\n")
    assert_equal("END", @sock_port_1.gets.chomp)
    assert_equal('{"localhost_11212"=>"STORED", "localhost_11211"=>"STORED"}', @sock_port_1.gets.chomp)
    @sock_port_1.write("stat log_shift_age\r\n")
    assert_equal( "stats.log_shift_age min" , @sock_port_1.gets.chomp);
    assert_equal("END", @sock_port_1.gets.chomp)

    # get & check new log_shift_size value on port 11212
    @sock_port_2.write("stat log_shift_age\r\n")
    assert_equal( "stats.log_shift_age min" , @sock_port_2.gets.chomp);
    assert_equal("END", @sock_port_2.gets.chomp)

  end

end
