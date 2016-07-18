class DbHelper
  MYSQL_CMD = "MYSQL_PWD=#{DbConfig[:password]} mysql -u #{DbConfig[:username]}"

  class << self
    def truncate(table)
      sql = "TRUNCATE TABLE `#{table}`"
      query(sql)
    end

    private

    def run(sql)
      cmd = "#{MYSQL_CMD} < #{sql}"
      unless system(cmd)
        raise "Failed to run sql : #{cmd}"
      end
    end

    def query(sql)
      client.query(sql)
    end

    def client
      @client ||= Mysql2::Client.new(DbConfig.to_hash)
    end
  end
end
