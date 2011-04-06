module Beanstalker
  class DaemonizerHandler < Daemonizer::Handler
    def prepare(starter, &block)
      logger.info "Loading Rails"
      require File.join(Daemonizer.root, '/config/environment')
      require 'beanstalker/worker'
      logger.info "Rails loaded"
      super
    end

    def start
      $logger = logger
      $logger.info "Starting cycle"
      if option(:error_handler)
        Worker.custom_error_handler = option(:error_handler)
      end
      if option(:timeout_handler)
        Worker.custom_timeout_handler = option(:timeout_handler)
      end
      if option(:before_filter)
        Worker.before_filter = option(:before_filter)
      end
      if option(:on_job_event)
        Worker.on_job_event = option(:on_job_event)
      end
      @worker = Worker.new(binding,
                :tube => option(:tube),
                :servers => option(:servers),
                :worker_id => worker_id,
                :workers_count => workers_count,
                :ruby_timeout => option(:ruby_timeout).nil? ? true : option(:ruby_timeout))
      @worker.run
      $logger.info "Ending cycle"
    end
  end
end
