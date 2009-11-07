# ROMA's logger.  
#
# rlogger.rb - it is an extension to a standard logger for ROMA
#
require 'logger'

module Roma
  module Logging

    class RLogger
      VERSION = '0.0.1'
      
      module Severity
        TRACE = -1
        DEBUG = 0
        INFO = 1
        WARN = 2
        ERROR = 3
        FATAL = 4
        UNKNOWN = 5
      end
      include Severity

      module ExtLogDev
        def extendLogDev()
          if @logdev 
            @logdev.extend(ExtShiftAge)
          end
        end
      end
  
      module ExtShiftAge
        private 
        def shift_log_period(now) 
          postfix = previous_period_end(now).strftime("%Y%m%d%H%M")
          age_file = "#{@filename}.#{postfix}"
          if FileTest.exist?(age_file)
            raise RuntimeError.new("'#{age_file}' already exists.")
          end
          @dev.close
          File.rename(@filename, age_file)
          @dev = create_logfile(@filename)
          return true
        end
    
        def previous_period_end(now)
          ret = nil
          case @shift_age
          when /^min$/
            t = now - 1 * 60
            ret = Time.mktime(t.year, t.month, t.mday, t.hour, t.min, 59)
          when /^hour$/
            t = now - 1 * 60 * 60
            ret = Time.mktime(t.year, t.month, t.mday, t.hour, 59, 59)
          when /^daily$/
            ret = eod(now - 1 * SiD)
          when /^weekly$/
            ret = eod(now - ((now.wday + 1) * SiD))
          when /^monthly$/
            ret = eod(now - now.mday * SiD)
          else 
            ret = now
          end
          return ret
        end
      end
  
      module ExtTrace
        private
        SEV_LABEL = %w{DEBUG INFO WARN ERROR FATAL ANY}
        
        def format_severity(severity)
          if @level <= RLogger::TRACE and severity <= RLogger::TRACE
            return 'TRACE'
          else 
            return SEV_LABEL[severity] || 'ANY'
          end
        end
      end

      @@singleton_instance = nil

      def self.create_singleton_instance(logdev, shift_age = 0, shift_size = 1048576)
        @@singleton_instance = RLogger.new(logdev, shift_age, shift_size)
        private_class_method  :new, :allocate
      end

      def self.instance
        @@singleton_instance
      end

      def initialize(logdev, shift_age = 0, shift_size = 1048576)
        @wrap_logger = Logger.new(logdev, shift_age, shift_size)
        @wrap_logger.extend(ExtTrace) 
        @wrap_logger.extend(ExtLogDev)
        @wrap_logger.extendLogDev()
      end
      
      def level=(severity)
        @wrap_logger.level = severity
      end
      
      def trace?; @wrap_logger.level <= TRACE; end 
      
      def debug?; @wrap_logger.debug?; end 
      
      def info?; @wrap_logger.info?; end
      
      def warn?; @wrap_logger.warn?; end
  
      def error?; @wrap_logger.error?; end
      
      def fatal?; @wrap_logger.fatal?; end
  
      def trace(progname = nil, &block)
        @wrap_logger.add(TRACE, nil, progname, &block)
      end
    
      def debug(progname = nil, &block)
        @wrap_logger.debug(progname, &block)
      end
  
      def info(progname = nil, &block)
        @wrap_logger.info(progname, &block)
      end
  
      def warn(progname = nil, &block)
        @wrap_logger.warn(progname, &block)
      end
  
      def error(progname = nil, &block)
        @wrap_logger.error(progname, &block)
      end
  
      def fatal(progname = nil, &block)
        @wrap_logger.fatal(progname, &block)
      end
  
      def unknown(progname = nil, &block)
        @wrap_logger.unknow(progname, &block)
      end
  
      def close; @wrap_logger.close; end
  
    end # class RLogger

  end # module Logging
end # module Roma
