require 'logger'

class MysqlImport
  class Logger < ::Logger
    def initialize(out, debug)
      super(out)

      @level = INFO unless debug
      case out
      when STDOUT, STDERR
        @formatter = ->(_, _, _, message) { "#{String === message ? message : message.inspect}\n" }
      end
    end
  end

  module Logging
    def initialize(config, opts = {})
      @debug = opts.fetch(:debug, false)
      @logger = if opts[:log].is_a?(::Logger)
                  opts[:log]
                else
                  Logger.new(opts[:log], @debug)
                end
      embed_logger
      super
    end

    def import(*filters)
      super
    ensure
      @logger.info('Imported tables:')
      if @result.imported.size > 0
        max_len = @result.imported.map(&:first).max_by{|w| w.length}.length
        @result.imported.sort.each do |t|
          space = ' ' * ((max_len - t[0].length) + 1)
          msg = "  #{t[0]}#{space}[exec:#{format('%.3fs', t[1])}"
          msg << " lock:#{format('%.3fs', t[2])}" if t[2]
          msg << ']'
          @logger.info(msg)
        end
      else
        @logger.info('  nothing...')
      end
      if @result.skipped.size > 0
        @logger.info('Skipped tables:')
        @result.skipped.sort.each {|t| @logger.info("  #{t}") }
      end

      @result.clear
    end

    private

    def parallel_opts
      @parallel_opts ||= if @debug
                           super.merge(
                             finish: proc do |item, index, _result|
                               @logger.debug("parallel_item: #{item.inspect}")
                               @logger.debug("parallel_index: #{index}")
                             end
                           )
                         else
                           super
                         end
    end

    def embed_logger
      if @debug && !LoadDataInfile2::Client.instance_methods.include?(:build_sql_with_logging)
        LoadDataInfile2::Client.class_exec(@logger) do |logger|
          define_method :build_sql_with_logging do |file, options = {}|
            build_sql_without_logging(file, options).tap {|sql| logger.debug("sql: #{sql}") }
          end
          alias_method :build_sql_without_logging, :build_sql
          alias_method :build_sql, :build_sql_with_logging
        end
      end
    end
  end

  prepend Logging
end
