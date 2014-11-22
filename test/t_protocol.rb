#!/usr/bin/env ruby

require 'socket'

class ProtocolTest < Test::Unit::TestCase
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

  def test_set_get_delete
    # Set data
    @sock.write("set key 0 0 5\r\nvalue\r\n")
    assert_equal("STORED", @sock.gets.chomp)

    # Get data
    @sock.write("get key\r\n")
    assert_equal("VALUE key 0 5", @sock.gets.chomp)
    assert_equal("value", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)

    # Update data
    @sock.write("set key 0 0 9\r\nnew_value\r\n")
    assert_equal("STORED", @sock.gets.chomp)

    # Confirm updated data
    @sock.write("get key\r\n")
    assert_equal("VALUE key 0 9", @sock.gets.chomp)
    assert_equal("new_value", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)

    # Delete data
    @sock.write("delete key\r\n")
    assert_equal("DELETED", @sock.gets.chomp)

    # Confirm deleted data
    @sock.write("get key\r\n")
    assert_equal("END", @sock.gets.chomp)
  end

  def test_gets
    # Set first data
    @sock.write("set key-1 0 0 7\r\nvalue-1\r\n")
    assert_equal("STORED", @sock.gets.chomp)

    # Gets data
    @sock.write("gets key-1 key-2\r\n")
    assert_equal("VALUE key-1 0 7 0", @sock.gets.chomp)
    assert_equal("value-1", @sock.gets.chomp)
    assert_equal("END", @sock.gets.chomp)

    # Set second data
    @sock.write("set key-2 0 0 7\r\nvalue-2\r\n")
    assert_equal("STORED", @sock.gets.chomp)

    # Gets data (order is not sorted)
    @sock.write("gets key-1 key-2\r\n")
    if(@sock.gets.include?("key-1"))
      assert_equal("value-1", @sock.gets.chomp)
      assert_equal("VALUE key-2 0 7 0", @sock.gets.chomp)
      assert_equal("value-2", @sock.gets.chomp)
    else
      assert_equal("value-2", @sock.gets.chomp)
      assert_equal("VALUE key-1 0 7 0", @sock.gets.chomp)
      assert_equal("value-1", @sock.gets.chomp)
    end
    assert_equal("END", @sock.gets.chomp)
  end

  def test_get_non_exisitent_key
    @sock.write("get key\r\n")
    assert_equal("END", @sock.gets.chomp)
  end

  def test_delete_non_exisitent_key
    @sock.write("delete key\r\n")
    assert_equal("NOT_FOUND", @sock.gets.chomp)
  end

  def test_gets_non_exisitent_key
    @sock.write("gets key\r\n")
    assert_equal("END", @sock.gets.chomp)
  end

  def test_set_zero_arguments
    @sock.write("get\r\n")
    assert(@sock.gets.chomp.start_with?("CLIENT_ERROR"))
  end

  def test_get_zero_arguments
    @sock.write("get\r\n")
    assert(@sock.gets.chomp.start_with?("CLIENT_ERROR"))
  end

  def test_delete_zero_arguments
    @sock.write("delete\r\n")
    assert(@sock.gets.chomp.start_with?("CLIENT_ERROR"))
  end

  def test_gets_zero_arguments
    @sock.write("gets\r\n")
    assert_equal("END", @sock.gets.chomp)
  end

  def test_set_too_much_arguments
    @sock.write("set key 0 0 5 0\r\nvalue\r\n")
    assert(@sock.gets.chomp.start_with?("CLIENT_ERROR"))
  end

  def test_set_wrong_key_size
    @sock.write("set key 0 0 -1\r\nvalue\r\n")
    assert(@sock.gets.start_with?("CLIENT_ERROR"))
  end

  def test_set_zero_byte_value
    @sock.write("set key 0 0 0\r\n\r\n")
    assert_equal("STORED", @sock.gets.chomp)
    @sock.write("get key\r\n")
    assert_equal("VALUE key 0 0", @sock.gets.chomp)
    assert_equal("", @sock.gets.chomp)
  end
end
