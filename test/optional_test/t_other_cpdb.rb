#!/usr/bin/env ruby
require 'roma/client/rclient'
require 'roma/messaging/con_pool'
require 'roma/config'
require 'pathname'

class CpdbBaseTest < Test::Unit::TestCase
  self.test_order = :defined
  include RomaTestUtils

  def teardown
    stop_roma
  rescue => e
    puts "#{e} #{$@}"
  end

end

# Dbm Storage Test
class DbmTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_dbm.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
    @sock = TCPSocket.new("localhost", 11211)
  end
  def test_st_class_dbm
      @sock.write("stat st_class\r\n")
      assert_equal('storages[roma].storage.st_class DbmStorage', @sock.gets.chomp)
      assert_equal('END', @sock.gets.chomp)
  end
end

# Sqlite3 Storage Test
class Sqlite3Test < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_sqlite3.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
    @sock = TCPSocket.new("localhost", 11211)
  end
  def test_st_class_sqlite3
      @sock.write("stat st_class\r\n")
      assert_equal('storages[roma].storage.st_class SQLite3Storage', @sock.gets.chomp)
      assert_equal('END', @sock.gets.chomp)
  end
end
