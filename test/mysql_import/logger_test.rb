require 'test_helper'

class MysqlImport::LoggerTest < Test::Unit::TestCase
  sub_test_case '#initialize' do
    def create_logger(log, debug = false)
      MysqlImport.new(DbConfig.to_hash, log: log, debug: debug).instance_variable_get(:@logger)
    end

    def get_logdev(logger)
      logger.__getobj__.instance_variable_get(:@logdev)
    end

    test 'file path' do
      logdev = get_logdev(create_logger('/tmp/mysql_import.log'))
      assert_equal File, logdev.instance_variable_get(:@dev).class
      assert_equal '/tmp/mysql_import.log', logdev.instance_variable_get(:@filename)
    end

    test 'nil' do
      assert_equal nil, get_logdev(create_logger(nil))
    end

    test 'STDOUT' do
      logdev = get_logdev(create_logger(STDOUT))
      assert_equal IO, logdev.instance_variable_get(:@dev).class
      assert_match Regexp.new('STDOUT'), logdev.instance_variable_get(:@dev).inspect
      assert_equal nil, logdev.instance_variable_get(:@filename)
    end

    test 'STDERR' do
      logdev = get_logdev(create_logger(STDERR))
      assert_equal IO, logdev.instance_variable_get(:@dev).class
      assert_match Regexp.new('STDERR'), logdev.instance_variable_get(:@dev).inspect
      assert_equal nil, logdev.instance_variable_get(:@filename)
    end

    test 'original' do
      mylogger = ::Logger.new('/tmp/mylogger.log')
      logger = create_logger(mylogger)
      logdev = get_logdev(logger)
      assert_equal mylogger.object_id, logger.__getobj__.object_id
      assert_equal File, logdev.instance_variable_get(:@dev).class
      assert_equal '/tmp/mylogger.log', logdev.instance_variable_get(:@filename)
    end

    test 'debug' do
      logger = create_logger('/tmp/mysql_import.log', true)
      assert_equal Logger::DEBUG, logger.level
    end
  end
end
