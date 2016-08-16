require 'test_helper'

require 'roma-test-storage'
require 'roma/storage/sqlite3_storage'

if ENV['OPTIONAL']
  class SQLite3StorageTest < Test::Unit::TestCase
    self.test_order = :defined
    include StorageTests

    def setup
      rmtestdir('storage_test')
      @st=Roma::Storage::SQLite3Storage.new
      @st.vn_list = [0,1,2,3,4,5,6,7,8,9]
      @st.storage_path = 'storage_test'
      @st.opendb
    end

    def test_close_after_each_clean_up
      h={}
      1000.times{|i|
        h[i.to_s]=[i%10,Time.now.to_i,0,Time.now.to_i].pack('NNNN')+'old data'
      }
      dmp=Marshal.dump(h)
      @st.load(dmp)

      assert_raise SQLite3::BusyException do
        @st.each_clean_up(Time.now.to_i-100, Hash.new(:primary) ){|k,vn|
          @st.closedb
        }
      end
    end
  end
end
