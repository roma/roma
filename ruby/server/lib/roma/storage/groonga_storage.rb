require 'groonga'
require 'roma/storage/basic_storage'

module Roma
  module Storage

    class GroongaStorage < BasicStorage
      def initialize
        super
        @ext_name = 'grn'
      end

      def get_stat
        ret = super
        @hdb.each_with_index do |hdb, i|
          ret["storage[#{i}].path"] = File.expand_path(hdb.path)
          ret["storage[#{i}].rnum"] = hdb.rnum
        end
        ret
      end

      private
      def open_db(fname)
        hdb = GroongaHash.new(fname)
        hdb.open
        hdb
      end

      def close_db(hdb)
        hdb.close
      end

      class GroongaHash
        def initialize(fname)
          @fname = fname
        end

        def path
          @hash.path
        end

        def put(key, value)
          record = @hash.add(key)
          @value[record.id] = value
        end

        def get(key)
          record = @hash[key]
          return nil if record.nil?
          @value[record.id]
        end

        def out(key)
          record = @hash[key]
          if record
            record.delete
            true
          else
            false
          end
        end

        def rnum
          @hash.count
        end

        def each
          @hash.each do |record|
            yield(record.key, @value[record.id])
          end
        end

        def open
          @context = Groonga::Context.new(:encoding => :none)
          if File.exist?(@fname)
            @database = Groonga::Database.new(@fname, :context => @context)
          else
            @database = Groonga::Database.create(:context => @context,
                                                 :path => @fname)
            Groonga::Schema.define(:context => @context) do |schema|
              schema.create_table("hash",
                                  :type => :hash,
                                  :key_type => "ShortText") do |table|
                table.text("value")
              end
            end
          end

          @hash = @context["hash"]
          @value = @hash.column("value")
        end

        def close
          @database.close
          @context.close
          @hash = @value = @database = @context = nil
        end
      end
    end # class GroongaStorage
  end # module Storage
end # module Roma
