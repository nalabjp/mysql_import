require 'logger'

class MysqlImport::Logger < SimpleDelegator
  def initialize(logger, debug = false)
    case logger
    when String
      obj = ::Logger.new(arg)
    when NilClass
      obj = ::Logger.new(STDOUT)
      obj.formatter = ->(seveity, datetime, progname, message) { "#{String === message ? message : message.inspect}\n" }
    when FalseClass
      obj = ::Logger.new('/dev/null')
    else
      obj = logger
    end

    if obj.respond_to?(:level=)
      obj.level = debug ? Logger::DEBUG : Logger::INFO
    end

    __setobj__(obj)
  end
end
