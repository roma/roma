require 'roma/storage/dbm'
require_relative 'config4cpdb_base'

module Roma
  module Config
    include CpdbBase::Config
    STORAGE_CLASS = Roma::Storage::DbmStorage
  end
end
