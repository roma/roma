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

class GroongaTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_groonga.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end

  def test_error_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage or RubyHashStorage system, your storage type is GroongaStorage", value)
  end
end

class RubyHashTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_rh.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_error_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("", value)
  end
end

## take 100 secs
class TcTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_tc.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_match(/safecopy_flushed/, value)
  end
end

class TcMemTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_tcmem.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage or RubyHashStorage system, your storage type is TCMemStorage" , value)
  end
end

class DbmTest < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_dbm.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage or RubyHashStorage system, your storage type is DbmStorage", value)
  end
end

class Sqlite3Test < CpdbBaseTest
  def setup
    start_roma 'cpdbtest/config4cpdb_sqlite3.rb'
    @rc=Roma::Client::RomaClient.new(["localhost_11211","localhost_11212"])
  end
  def test_cpdb
    value = `#{bin_dir}/cpdb 11211`.chomp
    assert_equal("ERROR:cpdb supports just TCStorage or RubyHashStorage system, your storage type is SQLite3Storage", value)
  end
end

