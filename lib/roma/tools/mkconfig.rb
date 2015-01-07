require 'pathname'
require 'yaml'
require 'fileutils'

module Roma

  class Mkconfig
    TREE_TOP = "menu"
    LIB_PATH = Pathname(__FILE__).dirname.parent.parent
    CONFIG_TEMPLATE_PATH = File.expand_path(File.join(LIB_PATH, "roma/config.rb"))
    CONFIG_OUT_PATH = File.expand_path(File.join(Pathname.pwd, "config.rb"))
    PLUGIN_DIR = File.expand_path(File.join(LIB_PATH, File.join("roma", "plugin")))
    BNUM_COEFFICIENT = 2 #reccomend1-4.
    TC_FILE = 10
    REDUNDANCY = 2
    DEFAULT_ROMA_CONNECTION = 12
    RUBY_CONNECTION = 10
    JAVA_CONNECTION = 30
    PHP_CONNECTION = 256
    KB = 1024
    GB = 1024 * 1024 * 1024
    OS_MEMORY_SIZE = 1 * GB
    END_MSG = ["exit", "quit", "balse"]

    class Base
      attr_accessor :all, :flag

      def initialize
        flag = false
        @all = YAML.load <<-YAML
        menu:
          name:
          path_name: menu
          message: Please select by number.
          choice:
            - Select storage
            - Select plugin
            - Calculate File Descriptor
            - Save
          next:
            - storage
            - plugin
            - language
            - save

        storage:
          name: selected_storage
          path_name: storage
          message: Which storage will you use?
          choice:
            - Ruby Hash
            - Tokyo Cabinet
          default: 1
          next:
            - menu
            - memory
        memory:
          name: memory_size_GB
          path_name:
          float_flg: on
          message: How big memory size in 1 server? Please measure in GB.
          default: 0.6
          next: process
        process:
          name: process_num
          path_name:
          message: How many run ROMA process per machine?
          default: 2
          next: server
        server:
          name: server_num
          path_name:
          message: How many machine run as ROMA server?
          default: 1
          next: data
        data:
          name: data_num
          path_name:
          message: How many data will you store?
          default: 10000
          next: menu

        plugin:
          name: selected_plugin
          path_name: plugin
          message: Please select which plugin will you use.(plugin_storage.rb was already set)
          choice:
            #{load_path(PLUGIN_DIR) << "Select all plugins"}
          default: 1
          next:
            #{
              r = Array.new
              load_path(PLUGIN_DIR).count.times{ r << "continue" }
              r << "menu"
              r
            }
          store_type: Array
        continue:
          name:
          path_name: 
          message: Will you use other plugin?
          choice:
            - Select more
            - No more
          default: 2
          next:
            - plugin
            - check_plugin
        check_plugin:
          name:
          path_name: 
          message: ROMA requires plugin_storage.rb or substitute plugin.Will you continue without plugin_storage.rb?
          choice:
            - Add plugin_storage.rb
            - Not necessary
          default: 2
          next:
            - add_plugin
            - menu

        language:
          name: client_language
          path_name:
          message: Please select programming language of client by number.
          choice:
            - Ruby
            - Java
            - PHP
          default: 3
          next:
            - fd_server
            - fd_server
            - fd_server
        fd_server:
          name: server_num
          path_name: FileDescriptor
          message: How many machine run as ROMA server?
          default: 1
          next: fd_client
        fd_client:
          name: client_num 
          path_name:
          message: How many machine run as ROMA client?
          default: 1
          next: menu

        save: END

        YAML
      end

      def keys
        all.keys
      end

      def [](s)
        all[s]
      end

      def load_path(path)
        ret = Array.new
        files = Dir::entries(path)
        files.delete("plugin_stub.rb") if files.include?("plugin_stub.rb")
        files.delete("plugin_storage.rb") if files.include?("plugin_storage.rb")

        files.each do |file|
          ret << file if File::ftype(File.join(path, file)) == "file"
        end

        ret
      end

      def print_question(key)
        target = all[key]
        #print question
        print "#{target["message"]}\n"
        if target.key?("choice")
          target["choice"].each_with_index do |k, i|
            print "[#{i + 1}] #{k}\n"
          end
        end
      end

      def next (key, input)
        target = all[key]["next"]

        if target.class == Array
          return target[input.to_i - 1]
        else
          return target
        end
      end
    end

    class Input
      module InputReadline
        def get_line
          return Readline.readline("> ", false)
        end
      end

      module InputSimple
        def get_line
          print ">"
          return gets.chomp!
        end
      end

      def initialize
        begin
          require "readline"
        rescue LoadError
          self.extend InputSimple
          return
        end
        self.extend InputReadline
      end
    end

    class Config_status
      attr_accessor :name, :value, :print, :default_value
      def initialize(hash, input, store_type = nil)
        @name = hash["name"]
        @value = input
        if store_type == "Array"
          @value = Array.new
          @value << input
        end
        @print = true
      end
    end

    class Box
      def self.print_edge(width)
        print "+"
          width.times { print "-" }
        print "+\n"
      end

      def self.print_with_box(arg)
        return if arg.count == 0

        if arg.class == Hash
          strs = Array.new
          arg.each do |k, v|
             strs << "#{k}: #{arg[k]}"
          end
          arg = strs
        end

        width = max_length(arg) + 1
        print_edge(width)

        arg.each do |s|
          print "|#{s}"
          (width - s.length).times do
            print " "
          end
          print "|\n"
        end

        print_edge(width)
      end

      private

      def self.max_length(arg)
        max = 0
        arg.each do |s|
          max = s.length if s.length > max
        end
        max
      end
    end

    class Calculate
      def self.get_bnum(res)
        res["server"] = res["fd_server"] if !res["server"]
        ans = res["data"].value.to_i * BNUM_COEFFICIENT * REDUNDANCY / res["server"].value.to_i / TC_FILE
        return ans
      end

      def self.get_xmsize_max(res)
        ans = (res["memory"].value.to_f * GB - OS_MEMORY_SIZE) / res["process"].value.to_i / TC_FILE
        if ans <= 0
          ans = res["memory"].value.to_f * GB / 2 / res["process"].value.to_i / TC_FILE
        end
        ans = ans.to_i
        return ans
      end

      def self.get_fd(res)
        res["fd_server"] = res["server"] if !res["fd_server"]
        res["fd_server"].value.to_i * connection_num(res) + (res["fd_client"].value.to_i- 1) * DEFAULT_ROMA_CONNECTION * 2
      end

      def self.connection_num(res)
        case res["language"].value
          when "Ruby"
            connection = RUBY_CONNECTION
          when "Java"
            connection = JAVA_CONNECTION
          when "PHP"
            connection = PHP_CONNECTION
        end

        return connection
      end
    end

    def initialize(mode = :no_menu)
      # confirming overwrite
      if File.exist?(CONFIG_OUT_PATH)
        print("Config.rb already exist in current directory. \nWill you overwrite?[y/n]")
        if gets.chomp! != "y"
          p "config.rb  were not created!"
          exit
        end
      end

      @base = Base.new
      @results = Hash::new
      @next_hash = TREE_TOP

      begin
        @defaults = load_config([:STORAGE_CLASS, :STORAGE_OPTION, :PLUGIN_FILES])
      rescue LoadError
        puts 'Not found config.rb file.'
        return
      rescue
        p $!
        puts "Content of config.rb is wrong."
        return
      end
      mkconfig(mode)
    end

    def load_config(targets)
      require CONFIG_TEMPLATE_PATH
      d_value = Hash.new
      Config.constants.each do |cnst|
        if targets.include?(cnst)
          d_value[cnst] = Config.const_get(cnst)
        end
      end
      return d_value
    end

    def mkconfig(mode)
      skip = skip_menu!(mode)

      while true
        clear_screen

        if @next_hash == "add_plugin"
          @results["plugin"].value.unshift("plugin_storage.rb")
          @next_hash = "menu"
        end

        skip.call if @next_hash == "menu" || @next_hash == "server" || @next_hash == "fd_server" || @next_hash == "check_plugin"
        break if end?(@base[@next_hash])
        puts "if you dosen't input anything, default value is set."
        Box.print_with_box(@defaults)
        print_status(@results)
        @base.print_question(@next_hash)
        input = get_input(@base[@next_hash])

        # if specific words(balse, exit, quit) was inputed, mkconfig.rb was finished.
        if END_MSG.include?(input)
          p "config.rb  were not created!"
          break

        else
          @results = store_result(@results, @base, @next_hash, input)
          @next_hash = @base.next(@next_hash, input)
        end
      end
    end

    def clear_screen
      print "\e[H\e[2J"
    end

    def skip_menu!(menu)
      # in case of "-m" or "--with_menu" option was used
      if menu == :with_menu
        return Proc.new do
          if @next_hash == "server" && @results["fd_server"]
            @next_hash = @base["server"]["next"]
          elsif @next_hash == "fd_server" && @results["server"]
            @next_hash = "fd_client"
          elsif @next_hash == "check_plugin" && @results["plugin"].value.include?("plugin_storage.rb")
            @next_hash = "menu"
          end
        end
      end

      # in case of "-m" or "--with_menu" option was NOT used
      i = 0
      return Proc.new do
        if @next_hash == "menu"
          @next_hash = @base["menu"]["next"][i]
          i += 1
        elsif @next_hash == "server" && @results["fd_server"]
          @next_hash = @base["server"]["next"]
        elsif @next_hash == "fd_server" && @results["server"]
          @next_hash = "fd_client"
        elsif @next_hash == "check_plugin" && @results["plugin"].value.include?("plugin_storage.rb")
          @next_hash = "language"
          i += 1
        end
      end
    end

    #judge whether data inputting finish or not
    def end?(s)
      if s == "END"
        save_data(@results)
        true
      end
    end

    def print_status(results)
      strs = Array.new
      results.each_value do |v|
        strs << "#{v.name} : #{v.value}"
      end
      Box.print_with_box(strs)
    end

    def get_input(hash)
      receiver = Input.new
      input = ""

      while !correct_in?(hash,input)
        input = receiver.get_line
        if input == ""
          #set defaults value
          input = hash["default"]
        end
      end

      input
    end

    def correct_in?(hash,input)
      if END_MSG.include?(input)
        return true
      end

      if hash["next"] == "continue"
        if (input == "y" || input == "n")
          return true
        end
      else
        if hash.key?('choice')
          if hash['choice'].count >= input.to_i && input.to_i > 0
            return true
          end
        else
          if hash["float_flg"]
            if 0 < input.to_f
              return true
            end
          else  
            if 0 < input.to_i
              return true
            end
          end
        end
      end

      return false
    end

    def store_result(results, base, hash, input)
      target = base[hash]

      return results if !target["name"]

      if target.key?("choice")
        if target["store_type"] == "Array"
          if base.flag
            results[hash].value << target["choice"][input.to_i - 1] if !results[hash].value.include?(target["choice"][input.to_i - 1])
          else
            results[hash] = Config_status.new(target, target["choice"][input.to_i - 1], target["store_type"])
            base.flag = true
          end

          if input.to_i == target["choice"].count
            results[hash].value = target["choice"][0..-2]
          end
        else
          results[hash] = Config_status.new(target, target["choice"][input.to_i - 1])
        end
      else
        results[hash] = Config_status.new(target, input)
      end

      base.flag = false if hash == "menu"
      return results
    end

    #make config.rb based on input data
    def save_data(res)
      if res.key?("storage")
        if res["storage"].value == "Ruby Hash"
          req = "rh_storage"
          storage = "RubyHashStorage"
        end

        if res["storage"].value == "Tokyo Cabinet"
          req = "tc_storage"
          storage = "TCStorage"
          bnum = Calculate.get_bnum(res)
          bnum = 5000000 if bnum < 5000000
          xmsiz = Calculate.get_xmsize_max(res)
        end
      end

      if res.key?("language")
        fd = Calculate.get_fd(res)
        print "\r\nPlease set FileDescriptor bigger than #{fd}.\r\n\r\n" 
      end

      body = ""
      open(CONFIG_TEMPLATE_PATH, "r") do |f|
        body = f.read
      end

      if req
        body = ch_assign(body, "require", " ", "roma\/storage\/#{req}")
        body = ch_assign(body, "STORAGE_CLASS", "Roma::Storage::#{storage}")

        if req == "rh_storage"
          body = ch_assign(body, "STORAGE_OPTION","")
        end

        if req == "tc_storage"
          body = ch_assign(body, "STORAGE_OPTION", "bnum=#{bnum}\#xmsiz=#{xmsiz}\#opts=d#dfunit=10")
        end
      end

      if res.key?("plugin")
        res["plugin"].value.unshift("plugin_storage.rb")
        body = ch_assign(body, "PLUGIN_FILES", res["plugin"].value)
      end

      open(CONFIG_OUT_PATH, "w") do |f|
        f.flock(File::LOCK_EX)
        f.puts body
        f.truncate(f.tell)
        f.flock(File::LOCK_UN)
      end

      puts "Before"
      Box.print_with_box(@defaults)

      re_require(CONFIG_OUT_PATH, Config)
      results = load_config([:STORAGE_CLASS, :STORAGE_OPTION, :PLUGIN_FILES])
      print "\r\nAfter\r\n"
      Box.print_with_box(results)
      print "\r\nMkconfig is finish.\r\n"
      print "\r\nIf you need, change directory path about LOG, RTTABLE, STORAGE, WB and other setting.\r\n\r\n"
    end

    # sep means separating right and left part(config.rb style)
    def ch_assign(text, exp, sep = " = ", str)
      sep = " = " if sep == "="
      text = text.gsub(/(\s*#{exp}).*/) do |s|
        name = $1
        if str.class == String
          if str =~ /::/ || str =~ /^\d+$/
            # storage type
           name + sep + str
          else
            # require & storage option
           name + sep + str.inspect
          end
        else
          # plugin
          # "to_s" equal "inspect" in Ruby 1.9
          name + sep + str.to_s.sub("\\", "")
        end
      end
    end

    def re_require(path, c_obj)
      $".delete(File.expand_path(path))
      c_obj.constants.each do |cnst|
        c_obj.class_eval { remove_const cnst }
      end
      require path
    end

  end # Mkconfig
end # module Roma
