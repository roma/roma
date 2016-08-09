require 'roma/storage/sqlite3_storage'
require_relative 'config4cpdb_base'

module Roma
  module Config
    include CpdbBase::Config
    STORAGE_CLASS = Roma::Storage::SQLite3Storage
  end
end
