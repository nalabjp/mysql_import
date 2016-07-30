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
end
