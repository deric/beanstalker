module Beanstalker
  class Mapper
    class Error < StandardError; end
    class WithBlockNotPresent < Error; end
    class NotAcceptable < Error; end
    class KindUnknown < NotAcceptable; end
    class TaskUnknown < NotAcceptable; end

    def initialize(*args)
      @mapping = {}
      @current_kind = nil
      args.each do |filename|
        include_from_file(filename)
      end
    end

    def include_from_file(filename)
      instance_eval(File.read(filename), filename)
    end

    def on(name, &block)
      $logger.info "We are processing #{@current_kind}/#{name} now"
      if @current_kind.nil?
        raise WithBlockNotPresent, "Wrap #on calls with #with block to setup :kind of task in beanstalker_mapper.rb"
      end
      @mapping[@current_kind] ||= {}
      @mapping[@current_kind][name.to_sym] = block
    end

    def with(kind, &block)
      @current_kind = kind.to_sym
      block.call
      @current_kind = nil
    end

    def can_handle_kind?(kind)
      @mapping && !! @mapping[kind.to_sym]
    end

    def method_for(kind, name)
      kind_mapping = @mapping[kind.to_sym]
      if kind_mapping.nil?
        raise KindUnknown, "No handler for kind = #{kind.to_sym.inspect}"
      end
      task_mapping = kind_mapping[name.to_sym]
      if task_mapping.nil?
        raise TaskUnknown, "No handler for task = #{name.to_sym.inspect}"
      end
      task_mapping
    end
  end
end