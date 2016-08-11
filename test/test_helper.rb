$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

if ENV['TRAVIS']
  begin
    require 'codeclimate-test-reporter'
    CodeClimate::TestReporter.start
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

require  File.expand_path('support/db_config.rb', __dir__)
require  File.expand_path('support/db_helper.rb', __dir__)

Test::Unit::TestCase.include(DbHelper::QueryInterface)
