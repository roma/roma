if ENV['STORAGE'] == 'groonga'
  require 'test_helper'

  require 'roma-test-storage'
  require 'roma/storage/groonga'

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
end
