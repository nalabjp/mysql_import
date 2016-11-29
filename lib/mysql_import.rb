require 'mysql_import/version'
require 'mysql_import/logger'
require 'load_data_infile2'
require 'connection_pool'
require 'parallel'

class MysqlImport
  def initialize(config, opts = {})
    @stash = []
    @lock = opts.fetch(:lock, false)
    @concurrency = opts.has_key?(:concurrency) ? opts[:concurrency].to_i : 2
    pool = @concurrency.zero? ? 1 : @concurrency
    sql_opts = opts.fetch(:sql_opts, {})

    @client = ConnectionPool.new(size: pool) { LoadDataInfile2::Client.new(config, sql_opts) }
    @result = Result.new
  end

  def add(file_path, opts = {})
    @stash.push([file_path, opts])
  end

  def import(filters = nil)
    Parallel.each(filtered_list(filters), parallel_opts) do |args|
      @client.with do |cli|
        run_import(cli, *args)
      end
    end
  end

  private

  def filtered_list(filters)
    return @stash if filters.nil?

    regexps = Array(filters).map{|f| Regexp.new(f) if f.is_a?(String) }.compact
    return [] if regexps.empty?

    @stash.map{|row| row if regexps.any?{|r| r.match(row[0]) } }.compact
  end

  def parallel_opts
    { in_threads: @concurrency }
  end

  def run_import(cli, fpath, opts)
    t = Time.now
    imported = false

    sql_opts = opts.reject {|k, _| %i(before after).include?(k) }
    table = sql_opts[:table] || File.basename(fpath, '.*')
    lock = opts.fetch(:lock, @lock)

    begin
      if lock
        write_lock(cli, table)
        lt = Time.now
      end

      run_action(opts[:before], cli)

      cli.import(fpath, sql_opts)
      imported = true

      run_action(opts[:after], cli)
    rescue Break
      @result.skipped.push(table) unless imported
    ensure
      res = [table, (Time.now - t)]
      if lock
        res.push(Time.now - lt)
        unlock(cli)
      end
      @result.imported.push(res) if imported
    end
  end

  def run_action(action, cli)
    return unless action

    case action
    when Array
      action.each { |act| run_action(act, cli) }
    when String
      cli.query(action)
    else
      action.call(cli)
    end
  end

  def write_lock(cli, table)
    cli.query("LOCK TABLE `#{table}` WRITE;")
  end

  def unlock(cli)
    cli.query("UNLOCK TABLES;")
  end

  class Result
    def imported
      @imported ||= []
    end

    def skipped
      @skipped ||= []
    end

    def clear
      imported.clear
      skipped.clear
    end
  end

  class Break < StandardError; end
end
