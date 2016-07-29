require 'logger'

class MysqlImport
  class Logger < SimpleDelegator
    def initialize(logger, debug)
      case logger
      when String
        obj = ::Logger.new(arg)
      when NilClass
        obj = ::Logger.new(STDOUT)
        obj.formatter = ->(seveity, datetime, progname, message) { "#{String === message ? message : message.inspect}\n" }
      when FalseClass
        obj = ::Logger.new('/dev/null')
      else
        obj = logger
      end

      obj.level = debug ? ::Logger::DEBUG : ::Logger::INFO
      __setobj__(obj)
    end
  end

  module Logging
    def initialize(config, opts ={}, sql_opts = {})
      @logger = Logger.new(opts[:log], opts.fetch(:debug, false))

      LoadDataInfile2::Client.class_exec(@logger) do |logger|
        define_method :build_sql_with_logging do |file, options|
          options = {} unless options
          build_sql_without_logging(file, options).tap {|sql| logger.debug("sql: #{sql}") }
        end
        alias_method :build_sql_without_logging, :build_sql
        alias_method :build_sql, :build_sql_with_logging
      end

      super
    end

    def import(*filters)
      super
    ensure
      logger.info('Imported tables:')
      if result.imported.size > 0
        result.imported.sort.each {|t| logger.info("  #{t[0]} (#{t[1]} sec)") }
      else
        result.logger.info('  nothing...')
      end
      if result.skipped.size > 0
        logger.info('Skipped tables:')
        result.skipped.sort.each {|t| logger.info("  #{t}") }
      end

      result.clear
    end

    private

    attr_reader :logger

    def parallel_opts
      @_parallel_opts ||= super.merge(
        finish: proc do |item, index, _result|
          logger.debug("parallel_item: #{item.inspect}")
          logger.debug("parallel_index: #{index}")
        end
      )
    end
  end

  prepend Logging
end
