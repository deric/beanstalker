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
      @worker = Worker.new(binding, 
                :tube => option(:tube), 
                :servers => option(:servers),
                :worker_id => worker_id, 
                :workers_count => workers_count)
      @worker.run
      $logger.info "Ending cycle"
    end
  end
end
