# MysqlImport

[![Gem Version](https://badge.fury.io/rb/mysql_import.svg)](https://badge.fury.io/rb/mysql_import)
[![Build Status](https://travis-ci.org/nalabjp/mysql_import.svg?branch=master)](https://travis-ci.org/nalabjp/mysql_import)
[![Code Climate](https://codeclimate.com/github/nalabjp/mysql_import/badges/gpa.svg)](https://codeclimate.com/github/nalabjp/mysql_import)
[![Test Coverage](https://codeclimate.com/github/nalabjp/mysql_import/badges/coverage.svg)](https://codeclimate.com/github/nalabjp/mysql_import/coverage)
[![Dependency Status](https://gemnasium.com/badges/github.com/nalabjp/mysql_import.svg)](https://gemnasium.com/github.com/nalabjp/mysql_import)

Simple concurrent importer for MySQL using [load_data_infile2](https://github.com/nalabjp/load_data_infile2).

## Installation

Add to your application's Gemfile:

```ruby
gem 'mysql_import'
```

And bundle.

## Examples
### Basic Usage

For exampole, if you want to import to `users` table from `/path/to/users.csv`:
```ruby
db_config = {
  host: 'localhost'
  database: 'mysql_import_test'
  username: 'root'
}
importer = MysqlImport.new(db_config)
importer.add('/path/to/users.csv')
importer.import
# => Import to `users` tables
```

Multiple import:
```ruby
importer = MysqlImport.new(db_config)
importer.add('/path/to/users.csv')
importer.add('/path/to/groups.csv')
importer.add('/path/to/departments.csv')
importer.import
# => Import to three tables from three csv files
```

MysqlImport has the concurrency because it uses the [parallel](https://github.com/grosser/parallel) gem.

With import options:

```ruby
importer = MysqlImport.new(db_config)
importer.add('/path/to/users1.csv', table: 'users')
importer.add('/path/to/users2.csv', table: 'users')
importer.add('/path/to/users3.csv', table: 'users')
importer.import
# => Import to `users` table from three csv files
```

See more details for import options.

https://github.com/nalabjp/load_data_infile2#sql-options

### Filter

If you want to import only a specific file, you can specify the file.

The specification of the file will be used regular expression

```ruby
importer = MysqlImport.new(db_config)
importer.add('/path/to/users.csv')
importer.add('/path/to/groups.csv')
importer.add('/path/to/departments.csv')
importer.import('users')
# => Only import to `users` table

importer.import('users', 'groups')
# => Import to `users` and `groups` table
```

### Hook

You are able to set the hook immediately before and after import.

The hook will accept either String or Proc or Array.

#### String

String is evaluated directly as SQL.

```ruby
importer = MysqlImport.new(db_config)
importer.add(
  '/path/to/users.csv',
  {
    before: 'TRUNCATE TABLE users;'
  }
)
importer.import
# => Truncate query is always executed before import.
```

#### Proc

If you want to make the subsequent processing based on the execution result of SQL, you should use Proc.

Arguments that are passed to Proc is an instance of `LoadDataInfile2::Client`, which is a subclass of `Mysql2::Client`.

```ruby
importer = MysqlImport.new(db_config)
importer.add(
  '/path/to/users.csv',
  {
    before: ->(cli) {
      res = cli.query('SELECT COUNT(*) AS c FROM users;')
      cli.query('TRUNCATE TABLE users;') if res.first['c'] > 0
    }
  }
)
importer.import
# => If there is one or more records in `users` table, truncate query is executed.
```

#### Array

Array of elements you need to use String or Proc.

```ruby
importer = MysqlImport.new(db_config)
importer.add(
  '/path/to/users.csv',
  {
    before: [
      "SET sql_mode = 'STRICT_TRANS_TABLES';",
      ->(cli) {
        res = cli.query('SELECT COUNT(*) AS c FROM users;')
        cli.query('TRUNCATE TABLE users;') if res.first['c'] > 0
      }
    ],
    after: [
      'SET @i = 0;',
      'UPDATE users SET order = (@i := @i + 1) ORDER BY name, email ASC;',
    ]
  }
)
importer.import
```

#### Skip all subsequent processing

If you want to skip all subsequent processing, you will need to raise `MysqlImport::Break` in Proc.

```ruby
importer = MysqlImport.new(db_config)
importer.add(
  '/path/to/users.csv',
  {
    before: ->(cli) {
      res = cli.query('SELECT COUNT(*) AS c FROM users;')
      raise MysqlImport::Break if res.first['c'] > 0
    },
    after: [
      'SET @i = 0;',
      'UPDATE users SET order = (@i := @i + 1) ORDER BY name, email ASC;',
    ]
  }
)
importer.import
# => If there is one or more records in `users` table, import and after hook will be skipped.
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/nalabjp/mysql_import.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

