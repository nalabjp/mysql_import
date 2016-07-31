require 'test_helper'

class MysqlImportTest < Test::Unit::TestCase
  test 'It has a version number' do
    refute_nil ::MysqlImport::VERSION
  end

  sub_test_case '#import' do
    teardown do
      DbHelper.truncate('users')
    end

    def create_client(opts = {})
      default_opts = { sql_opts: { local_infile: true } }
      MysqlImport.new(DbConfig.to_hash, default_opts.merge(opts))
    end

    test 'success' do
      assert_equal 0, dbh_query('select * from users;').size

      client = create_client
      client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
      client.import

      res = dbh_query('select * from users')
      assert_equal 1, res.size
      assert_equal 1, res.first['id']
      assert_equal 'nalabjp', res.first['name']
      assert_equal 'nalabjp@gmail.com', res.first['email']
    end

    sub_test_case 'filtering' do
      test 'full file name' do
        assert_equal 0, dbh_query('select * from users;').size

        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
        client.add(File.expand_path('../csv/not_import.csv', __FILE__), table: 'users')
        client.import('users_valid.csv')

        res = dbh_query('select * from users')
        assert_equal 1, res.size
      end

      test 'partial file name' do
        assert_equal 0, dbh_query('select * from users;').size

        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
        client.add(File.expand_path('../csv/not_import.csv', __FILE__), table: 'users')
        client.import('users')

        res = dbh_query('select * from users')
        assert_equal 1, res.size
      end
    end
  end
end
