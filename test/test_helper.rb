$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

if ENV['TRAVIS']
  begin
    require 'codeclimate-test-reporter'
    CodeClimate::TestReporter.start
    require 'simplecov'
    SimpleCov.start
  rescue LoadError
  end
end

begin
  require 'pry'
rescue LoadError
end

require 'mysql2'
require 'mysql_import'
require 'test/unit'

Dir[File.join(File.dirname(__FILE__), 'support/**/**.rb')].each {|f| require f }

Test::Unit::TestCase.include(DbHelper::QueryInterface)
