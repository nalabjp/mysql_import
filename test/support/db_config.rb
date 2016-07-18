require 'yaml'
require 'erb'

class DbConfig
  class << self
    def to_hash(local_infile: true)
      dup = Marshal.load(Marshal.dump(config))
      dup.delete('local_infile') unless local_infile
      dup
    end

    def [](key)
      config[key.to_s]
    end

    private

    def config
      @config ||= load_config
    end

    def load_config
      yaml = File.new(File.expand_path('../../config/database.yml', __FILE__))
      hash = YAML.load(ERB.new(yaml.read).result)
      hash.fetch('test')
    end
  end
end
