require 'test_helper'
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

# Groonga Storage Test
class GroongaTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_groonga.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
    @sock = TCPSocket.new("localhost", 11211)
  end

  def test_st_class_grn
    @sock.write("stat st_class\r\n")
    assert_equal("storages[roma].storage.st_class GroongaStorage", @sock.gets.chomp)
    assert_equal('END', @sock.gets.chomp)
  end
end

# RubyHash Storage Test
class RubyHashTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_rh.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
    @sock = TCPSocket.new("localhost", 11211)
  end
  def test_st_class_rh
      @sock.write("stat st_class\r\n")
      assert_equal('storages[roma].storage.st_class RubyHashStorage', @sock.gets.chomp)
      assert_equal('END', @sock.gets.chomp)
  end
end

#  TcTest Storage Test
class TcTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_tc.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
    @sock = TCPSocket.new("localhost", 11211)
  end
  def test_cpdb
    # Log Assertion
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_match(/safecopy_flushed/, value)
    assert_match(/finished/, value)
    # File exist Assertion
    valueFileList = `ls ./localhost_11211/roma/`.chomp
    assert_match(/9.tc.([\d]+)/, valueFileList)
  end
  def test_st_class_tc
      @sock.write("stat st_class\r\n")
      assert_equal('storages[roma].storage.st_class TCStorage', @sock.gets.chomp)
      assert_equal('END', @sock.gets.chomp)
  end
end

# TcMem Storage Test
class TcMemTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_tcmem.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
    @sock = TCPSocket.new("localhost", 11211)
  end
  def test_st_class_tcmem
      @sock.write("stat st_class\r\n")
      assert_equal('storages[roma].storage.st_class TCMemStorage', @sock.gets.chomp)
      assert_equal('END', @sock.gets.chomp)
  end
end
