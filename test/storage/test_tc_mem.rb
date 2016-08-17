require 'test_helper'

require 'roma-test-storage'
require 'roma/storage/tc_storage'

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
