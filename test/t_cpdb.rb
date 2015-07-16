#!/usr/bin/env ruby
require 'roma/client/rclient'
require 'roma/messaging/con_pool'
require 'roma/config'
require 'pathname'

class CpdbBaseTest < Test::Unit::TestCase
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
  end

  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage system, your storage type is GroongaStorage", value)
  end
end

# RubyHash Storage Test
class RubyHashTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_rh.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage system, your storage type is RubyHashStorage", value)
  end
end

#  TcTest Storage Test
class TcTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_tc.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
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
end

# TcMem Storage Test
class TcMemTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_tcmem.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage system, your storage type is TCMemStorage" , value)
  end
end

# Dbm Storage Test
class DbmTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_dbm.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage system, your storage type is DbmStorage", value)
  end
end

# Sqlite3 Storage Test
class Sqlite3Test < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_sqlite3.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage system, your storage type is SQLite3Storage", value)
  end
end

