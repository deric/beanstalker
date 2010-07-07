module AsyncObserver
  class DaemonizerHandler
    def self.before_init(logger, block)
      require File.join(Daemonizer.root, '/config/environment')
      require 'async_observer/worker'
      block.call
    end
    
    def self.after_init(logger, worker_id, worket_count)
      $logger = logger
      logger.info "Starting cycle"
      Worker.new(binding).run
      logger.info "Ending cycle"
    end
  end
end
