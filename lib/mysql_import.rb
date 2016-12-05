require 'mysql_import/version'
require 'mysql_import/logger'
require 'load_data_infile2'
require 'connection_pool'
require 'parallel'
require 'benchmark'

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
        begin
          store[:client] = cli
          import_internal(*args)
        ensure
          clear_store
        end
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

  def import_internal(fpath, opts)
    sql_opts = opts.reject {|k, _| %i(before after).include?(k) }
    store[:table] = sql_opts[:table] || File.basename(fpath, '.*')

    with_recording do
      with_lock_if_needed(opts.fetch(:lock, @lock)) do
        with_skip_handling do
          run_before_action(opts[:before])

          store[:client].import(fpath, sql_opts)

          run_after_action(opts[:after])
        end
      end
    end
  end

  def with_recording
    store[:exec_time] = realtime { yield }
  ensure
    if store[:before_break]
      @result.skipped.push(store[:table])
    else
      res = [store[:table], store[:exec_time]]
      res.push(store[:lock_time]) if store[:lock_time]
      @result.imported.push(res)
    end
  end

  def with_lock_if_needed(need)
    if need
      begin
        write_lock
        store[:lock_time] = realtime { yield }
      ensure
        unlock
      end
    else
      yield
    end
  end

  def with_skip_handling
    yield
  rescue BeforeBreak
    store[:before_break] = true
  rescue AfterBreak
    store[:after_break] = true
  end

  def realtime
    Benchmark.realtime { yield }
  end

  def run_action(action)
    return unless action

    case action
    when Array
      action.each { |act| run_action(act) }
    when String
      store[:client].query(action)
    else
      action.call(store[:client])
    end
  end

  def run_before_action(action)
    run_action(action)
  rescue Break
    raise BeforeBreak
  end

  def run_after_action(action)
    run_action(action)
  rescue Break
    raise AfterBreak
  end

  def write_lock
    [
      'SET @old_autocommit=@@autocommit;',
      'SET autocommit=0;',
      "LOCK TABLE `#{store[:table]}` WRITE;"
    ].each {|sql| store[:client].query(sql)}
  end

  def unlock
    [
      'COMMIT;',
      'UNLOCK TABLES;',
      'SET autocommit=@old_autocommit;'
    ].each {|sql| store[:client].query(sql)}
  end

  def store
    Thread.current[:store] ||= {}
  end

  def clear_store
    Thread.current[:store] = nil
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
  class BeforeBreak < Break; end
  class AfterBreak < Break; end
end
