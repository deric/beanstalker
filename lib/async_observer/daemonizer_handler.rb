module AsyncObserver
  class DaemonizerHandler < Daemonizer::Handler
    def before_init(block)
      require File.join(Daemonizer.root, '/config/environment')
      require 'async_observer/worker'
      super
    end
    
    def after_init
      $logger = logger
      logger.info "Starting cycle"
      logger.info "Options - #{option(:queue)}"
      Worker.new(binding, 
                :tube => option(:tube), 
                :servers => option(:servers),
                :worker_id => worker_id, 
                :workers_count => workers_count).run
      logger.info "Ending cycle"
    end
  end
end
