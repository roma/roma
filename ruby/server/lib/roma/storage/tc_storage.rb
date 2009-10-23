require 'tokyocabinet'
require 'roma/storage/basic_storage'

module Roma
  module Storage

    class TCStorage < BasicStorage
      include TokyoCabinet

      def initialize
        super
        @ext_name = 'tc'
      end

      def get_stat
        ret = super
        @hdb.each_with_index{|hdb,idx|
          ret["storage[#{idx}].path"] = File.expand_path(hdb.path)
          ret["storage[#{idx}].rnum"] = hdb.rnum
          ret["storage[#{idx}].fsiz"] = hdb.fsiz
        }
        ret
      end

      protected

      def set_options(hdb)
        prop = parse_options

        prop.each_key{|k|
          unless /^(bnum|apow|fpow|opts|xmsiz|rcnum|dfunit)$/ =~ k
            raise RuntimeError.new("Syntax error, unexpected option #{k}")
          end
        }
        
        opts = 0
        if prop.key?('opts')
          opts |= HDB::TLARGE if prop['opts'].include?('l')
          opts |= HDB::TDEFLATE if prop['opts'].include?('d')
          opts |= HDB::TBZIP if prop['opts'].include?('b')
          opts |= HDB::TTCBS if prop['opts'].include?('t')
        end

        hdb.tune(prop['bnum'].to_i,prop['apow'].to_i,prop['fpow'].to_i,opts)

        hdb.setxmsiz(prop['xmsiz'].to_i) if prop.key?('xmsiz')
        hdb.setcache(prop['rcnum'].to_i) if prop.key?('rcnum')
        hdb.setdfunit(prop['dfunit'].to_i) if prop.key?('dfunit')
      end

      private

      def parse_options
        return Hash.new(-1) unless @option
        buf = @option.split('#')
        prop = Hash.new(-1)
        buf.each{|equ|
          if /(\S+)\s*=\s*(\S+)/ =~ equ
            prop[$1] = $2
          else
            raise RuntimeError.new("Option string parse error.")
          end
        }
        prop
      end

      def open_db(fname)
        hdb = HDB::new

        set_options(hdb)

        if !hdb.open(fname, HDB::OWRITER | HDB::OCREAT | HDB::ONOLCK)
          ecode = hdb.ecode
          raise RuntimeError.new("tcdb open error #{hdb.errmsg(ecode)}")
        end
        hdb
      end

      def close_db(hdb)
        if !hdb.close
          ecode = hdb.ecode
          raise RuntimeError.new("tcdb close error #{hdb.errmsg(ecode)}")
        end        
      end

    end # class TCStorage

    class TCAsyncStorage < TCStorage
      private

      def open_db(fname)
        hdb = HDB::new

        set_options(hdb)

        hdb.instance_eval{
          alias put putasync
        }

        if !hdb.open(fname, HDB::OWRITER | HDB::OCREAT)
          ecode = hdb.ecode
          raise RuntimeError.new("tcdb open error #{hdb.errmsg(ecode)}")
        end

        Thread.new {
          loop{
            sleep 10
            hdb.sync
          }
        }

        hdb
      end
    end # class TCAsyncStorage


    class TCMemStorage < BasicStorage
      include TokyoCabinet

      private

      def open_db(fname)
        hdb = ADB::new
        hdb.open("*")
        hdb
      end

      def close_db(hdb); end

    end # class TCMemStorage

  end # module Storage
end # module Roma
