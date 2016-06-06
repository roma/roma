#!/usr/bin/env ruby

require 'timeout'

module Roma
  class CheckTc
    def initialize(storage_path, library_path, bin_path="")
      @storage_path = storage_path
      @library_path = library_path
      @bin_path = bin_path.empty? ? "#{@library_path}/bin/tchmgr" : bin_path
    end

    def check_flag
      status = {}
      timeout(5){
        Dir.glob("#{@storage_path}/*.tc").each{|f|
          res = `#{@bin_path} inform #{f}`
          res =~ /additional flags:(.*)\n/
          status.store(f, $1)
        }
      }
      status
    end

  end # CheckTc
end # Roma
