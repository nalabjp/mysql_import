require 'test_helper'

class MysqlImportTest < Test::Unit::TestCase
  test 'It has a version number' do
    refute_nil ::MysqlImport::VERSION
  end

  sub_test_case '#import' do
    teardown do
      DbHelper.truncate('users')
    end

    test 'success' do
      client = MysqlImport.new(DbConfig.to_hash)
      client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
      client.import
    end
  end
end
