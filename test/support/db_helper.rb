class DbHelper
  MYSQL_CMD = "MYSQL_PWD=#{DbConfig[:password]} mysql -u #{DbConfig[:username]}"

  class << self
    def truncate(table)
      sql = "TRUNCATE TABLE `#{table}`"
      query(sql)
    end

    def query(sql)
      client.query(sql)
    end

    private

    def client
      @client ||= Mysql2::Client.new(DbConfig.to_hash)
    end
  end

  module QueryInterface
    def dbh_query(sql)
      DbHelper.query(sql)
    end

    def dbh_find(sql)
      DbHelper.query(sql).first
    end

    def dbh_all(sql)
      DbHelper.query(sql).to_a
    end
  end
end
