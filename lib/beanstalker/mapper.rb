module Beanstalker
  class Mapper
    def initialize(*args)
      @mapping = {}
      args.each do |filename|
        include_from_file(filename)
      end
    end

    def include_from_file(filename)
      puts "Loading #{filename}"
      instance_eval(File.read(filename), filename)
    end

    def on(name, &block)
      @mapping[name.to_sym] = block
    end

    def method_for(name)
      @mapping[name.to_sym]
    end
  end
end