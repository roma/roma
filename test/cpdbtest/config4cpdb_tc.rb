require 'roma/storage/tc_storage'
require 'cpdbtest/config4cpdb_base'

module Roma
  module Config
    include CpdbBase::Config
    STORAGE_CLASS = Roma::Storage::TCStorage
  end
end
