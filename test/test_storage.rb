require 'test_helper'

require 'roma-test-storage'
require 'roma/storage/tc_storage'
require 'roma/storage/rh_storage'
require 'roma/storage/groonga_storage'

class TCStorageTest < Test::Unit::TestCase
  self.test_order = :defined
  include StorageTests

  def setup
    rmtestdir('storage_test')
    @st=Roma::Storage::TCStorage.new
    @st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @st.storage_path = 'storage_test'
    @st.opendb
  rescue =>e
    p e
  end

  def teardown
    @st.closedb
    rmtestdir('storage_test')
  end
end

class RubyHashStorageTest < Test::Unit::TestCase
  self.test_order = :defined
  include StorageTests

  def setup
    @st=Roma::Storage::RubyHashStorage.new
    @st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @st.opendb
  end

  def teardown
  end

  def test_cmp_clk
    (0x001E00000..0x002000000).each{|clk|
      assert_equal(0, @st.send(:cmp_clk,clk, clk) )
    }

    (0x001E00000..0x002000000).each{|clk|
      assert_operator(0,:>, @st.send(:cmp_clk,clk-1, clk) )
      assert_operator(0,:<, @st.send(:cmp_clk,clk, clk-1) )
    }

    (0x001E00000..0x002000000).each{|clk|
      assert_operator(0,:<, @st.send(:cmp_clk,clk+1, clk) )
      assert_operator(0,:>, @st.send(:cmp_clk,clk, clk+1) )
    }
    # t1=0 t2=0 clk2=0b0000...
    clk1=0x00000010
    clk2=0x00000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=1 clk2=0b0010...
    clk2=0x20000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=2 clk2=0b0100...
    clk2=0x40000000
    assert_operator(0, :>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0, :<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=3 clk2=0b0110...
    clk2=0x60000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=4 clk2=0b1000...
    clk2=0x80000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )

    # t1=0 t2=5 clk2=0b1010...
    clk2=0xa0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=6 clk2=0b1100...
    clk2=0xc0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2))
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1))
    # t1=0 t2=7 clk2=0b1110...
    clk2=0xe0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
  end

end

class TCMemStorageTest < Test::Unit::TestCase
  self.test_order = :defined
  include StorageTests

  def setup
    rmtestdir('storage_test')
    @st=Roma::Storage::TCMemStorage.new
    @st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @st.storage_path = 'storage_test'
    @st.opendb
  end
end

class GroongaStorageTest < Test::Unit::TestCase
  self.test_order = :defined
  include StorageTests

  def storage_path
    'groonga_storage_test'
  end

  def setup
    rmtestdir(storage_path)
    @st = Roma::Storage::GroongaStorage.new
    @st.vn_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    @st.storage_path = storage_path
    @st.opendb
  end

  def teardown
    @st.closedb
    rmtestdir(storage_path)
  end
end
