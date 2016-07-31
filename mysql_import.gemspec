lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mysql_import/version'

Gem::Specification.new do |spec|
  spec.name          = 'mysql_import'
  spec.version       = MysqlImport::VERSION
  spec.authors       = ['nalabjp']
  spec.email         = ['nalabjp@gmail.com']

  spec.summary       = 'Simple concurrent importer for MySQL'
  spec.description   = 'Simple concurrent importer for MySQL'
  spec.homepage      = 'https://github.com/nalabjp/mysql_import'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency 'load_data_infile2', '~> 0.2'
  spec.add_dependency 'connection_pool'
  spec.add_dependency 'parallel'

  spec.add_development_dependency 'bundler', '~> 1.12'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'test-unit'
end
