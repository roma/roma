require 'test_helper'

require 'roma-test-storage'
require 'roma/storage/tokyocabinet'

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
