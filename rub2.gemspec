
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "rub2/version"

Gem::Specification.new do |spec|
  spec.name          = "rub2"
  spec.version       = Rub2::VERSION
  spec.authors       = ["holrock"]
  spec.email         = ["ispeporez@gmail.com"]

  spec.summary       = "simple DSL for submitting jobs via qsub"
  spec.description   = ""
  spec.homepage      = "https://github.com/holrock/rub2"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.16"
  spec.add_development_dependency "rake", "~> 10.0"
end
