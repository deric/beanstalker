require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "beanstalker"
    gem.summary = %Q{Beanstalker provides deep integration with Beanstalk. Fork from http://github.com/kristjan/async_observer}
    gem.description = %Q{Beanstalker is a tool for executing long tasks in background in our rails application.}
    gem.email = "glebpom@gmail.com"
    gem.homepage = "http://github.com/glebpom/beanstalker"
    gem.authors = ["Gleb Pomykalov"]
    gem.add_dependency "daemonizer", "~>0.4.0"
    gem.add_dependency "beanstalk-client"
    gem.add_dependency "rails", ">= 2.2.0"
    gem.add_dependency "SystemTimer", "~>1.2.3"
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/test_*.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test => :check_dependencies

task :default => :test

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "beanstalker #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
