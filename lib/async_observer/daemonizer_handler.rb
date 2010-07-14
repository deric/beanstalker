module AsyncObserver
  class DaemonizerHandler < Daemonizer::Handler
    def prepare(block)
      logger.info "Loading Rails"
      require File.join(Daemonizer.root, '/config/environment')
      require 'async_observer/worker'
      logger.info "Rails loaded"
      super
    end
    
    def start
      logger.info "Starting cycle"
      Worker.new(binding, 
                :tube => option(:tube), 
                :servers => option(:servers),
                :worker_id => worker_id, 
                :workers_count => workers_count).run
      logger.info "Ending cycle"
    end
  end
end
