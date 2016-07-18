require 'mysql_import/version'
require 'load_data_infile2'
require 'connection_pool'
require 'parallel'

class MysqlImport
  def initialize(config, opts ={}, sql_opts = {})
    @stash = []
    @fileters = []
    @concurrency = opts.has_key?(:concurrency) ? opts[:concurrency].to_i : 2
    pool = concurrency.zero? ? 1 : concurrency

    @client = ConnectionPool.new(size: pool) { LoadDataInfile2::Client.new(config, sql_opts) }
    @result = Result.new
  end

  def add(file_path, options = {})
    stash.push([file_path, options])
  end

  def import(*filters)
    Parallel.each(filtered_list(filters), in_threads: concurrency) do |args|
      client.with do |cli|
        run_import(cli, *args)
      end
    end
  end

  private

  attr_reader :stash, :filters, :concurrency, :client, :result

  def filtered_list(filters)
    return stash if filters.empty?

    regexps = filters.map{|f| Regexp.new(f) }
    stash.map{|row| row if regexps.any?{|r| r.match(row[0]) } }.compact
  end

  def run_import(cli, fpath, opts)
    before = opts.delete(:before)
    after = opts.delete(:after)
    table = opts[:table] || File.basename(fpath, '.*')

    if before
      begin
        run_action(before, cli)
      rescue Break
        result.add(:skipped, table)
        return
      end
    end

    res = cli.import(fpath, opts)
    result.add(:imported, table)

    if after
      begin
        run_action(after, cli, res)
      rescue Break
      end
    end
  end

  def run_action(action, cli, res = nil)
    case action
    when Array
      action.map { |act| run_action(act, cli, res) }
    when String
      cli.query(action)
    else
      if res
        action.call(cli, res)
      else
        action.call(cli)
      end
    end
  end

  class Result
    def imported
      @imported ||= []
    end

    def skipped
      @skipped ||= []
    end

    def mutex
      @mutext ||= Mutex.new
    end

    def add(meth, res)
      mutex.synchronize { __send__(meth).push(res) }
    end
  end

  class Break < StandardError; end
end
