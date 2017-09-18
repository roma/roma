#!/usr/bin/env ruby

require 'timeout'

module Roma
  class CheckTc
    def initialize(storage_path, library_path)
      @storage_path = storage_path
      @library_path = library_path
    end

    def check_flag
      status = {}
     Timeout.timeout(5){
        Dir.glob("#{@storage_path}/*.tc").each{|f|
          res = `#{@library_path}/bin/tchmgr inform #{f}`
          res =~ /additional flags:(.*)\n/
          status.store(f, $1)
        }
      }
      status
    end

  end # CheckTc
end # Roma
