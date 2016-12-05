require 'test_helper'
require 'thwait'

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

        assert_equal 1, dbh_query('select * from users').size
      end

      test 'partial file name' do
        assert_equal 0, dbh_query('select * from users;').size

        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
        client.add(File.expand_path('../csv/not_import.csv', __FILE__), table: 'users')
        client.import('users')

        assert_equal 1, dbh_query('select * from users').size
      end

      test 'nil' do
        assert_equal 0, dbh_query('select * from users;').size

        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
        client.import(nil)

        assert_equal 1, dbh_query('select * from users').size
      end

      test 'empty String' do
        assert_equal 0, dbh_query('select * from users;').size

        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
        client.import('')

        assert_equal 1, dbh_query('select * from users').size
      end

      test 'empty Array' do
        assert_equal 0, dbh_query('select * from users;').size

        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
        client.import([])

        assert_equal 0, dbh_query('select * from users').size
      end
    end

    sub_test_case 'before action' do
      test 'sql' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          before: 'insert into users (id, name, email) values (100, "nalabjp100", "nalabjp100@gmail.com");'
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), opts)
        client.import

        res = dbh_query('select * from users').to_a
        assert_equal 2, res.size
        assert_equal 1, res[0]['id']
        assert_equal 'nalabjp', res[0]['name']
        assert_equal 'nalabjp@gmail.com', res[0]['email']
        assert_equal 100, res[1]['id']
        assert_equal 'nalabjp100', res[1]['name']
        assert_equal 'nalabjp100@gmail.com', res[1]['email']
      end

      test 'proc' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          before: ->(c) {
            res = c.query('select count(*) as c from users;')
            assert_equal 0, res.first['c']
          }
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), opts)
        client.import

        assert_equal 1, dbh_query('select * from users').size
      end

      test 'array' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          before: [
            ->(c) {
              res = c.query('select @my_var;')
              assert_equal nil, res.first['@my_var']
            },
            'set @my_var = 1;',
            ->(c) {
              res = c.query('select @my_var;')
              assert_equal 1, res.first['@my_var']
            }
          ]
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), opts)
        client.import

        assert_equal 1, dbh_query('select * from users').size
      end

      test 'skip' do
        assert_equal 0, dbh_query('select * from users;').size

        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), table: 'users')
        client.import

        assert_equal 1, dbh_query('select * from users').size

        opts = {
          table: 'users',
          before: [
            ->(c) {
              res = c.query('select count(*) as c from users;')
              raise MysqlImport::Break if res.first['c'] > 0
            }
          ]
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid_2records.csv', __FILE__), opts)
        client.import

        assert_equal 1, dbh_query('select * from users').size
      end
    end

    sub_test_case 'after action' do
      test 'sql' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          after: 'insert into users (id, name, email) values (100, "nalabjp100", "nalabjp100@gmail.com");'
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), opts)
        client.import

        res = dbh_query('select * from users').to_a
        assert_equal 2, res.size
        assert_equal 1, res[0]['id']
        assert_equal 'nalabjp', res[0]['name']
        assert_equal 'nalabjp@gmail.com', res[0]['email']
        assert_equal 100, res[1]['id']
        assert_equal 'nalabjp100', res[1]['name']
        assert_equal 'nalabjp100@gmail.com', res[1]['email']
      end

      test 'proc' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          after: ->(c) {
            c.query('update users set name = "nalabjp-updated" where id = 1;')
          }
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), opts)
        client.import

        res = dbh_query('select * from users').to_a
        assert_equal 1, res.size
        assert_equal 1, res[0]['id']
        assert_equal 'nalabjp-updated', res[0]['name']
        assert_equal 'nalabjp@gmail.com', res[0]['email']
      end

      test 'array' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          after: [
            ->(c) {
              res = c.query('select @my_var;')
              assert_equal nil, res.first['@my_var']
            },
            'set @my_var = 1;',
            ->(c) {
              res = c.query('select @my_var;')
              assert_equal 1, res.first['@my_var']
            }
          ]
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), opts)
        client.import

        assert_equal 1, dbh_query('select * from users').size
      end

      test 'skip' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          after: [
            ->(c) {
              res = c.query('select count(*) as c from users;')
              raise MysqlImport::Break if res.first['c'] > 1
              c.query('truncate table users;')
            }
          ]
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid_2records.csv', __FILE__), opts)
        client.import

        assert_equal 2, dbh_query('select * from users').size
      end
    end

    sub_test_case 'with write lock' do
      test 'enable in initialization' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          before: 'select sleep(2);'
        }
        client = create_client(lock: true)
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), opts)

        th_ret_before = nil
        th_ret_after = nil
        th = Thread.new do
          th_ret_before = Mysql2::Client.new(DbConfig.to_hash).query('select * from users;').size
          sleep(2)
          th_ret_after = Mysql2::Client.new(DbConfig.to_hash).query('select * from users;').size
        end
        thall = ThreadsWait.new(th)

        sleep(1)
        client.import
        assert_equal 1, dbh_query('select * from users').size

        thall.all_waits
        assert_equal 0, th_ret_before
        assert_equal 1, th_ret_after
      end

      test 'enable in options' do
        assert_equal 0, dbh_query('select * from users;').size

        opts = {
          table: 'users',
          lock: true,
          before: 'select sleep(2);'
        }
        client = create_client
        client.add(File.expand_path('../csv/users_valid.csv', __FILE__), opts)

        th_ret_before = nil
        th_ret_after = nil
        th = Thread.new do
          th_ret_before = Mysql2::Client.new(DbConfig.to_hash).query('select * from users;').size
          sleep(2)
          th_ret_after = Mysql2::Client.new(DbConfig.to_hash).query('select * from users;').size
        end
        thall = ThreadsWait.new(th)

        sleep(1)
        client.import
        assert_equal 1, dbh_query('select * from users').size

        thall.all_waits
        assert_equal 0, th_ret_before
        assert_equal 1, th_ret_after
      end
    end
  end
end
