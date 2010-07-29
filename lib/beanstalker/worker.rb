# async-observer - Rails plugin for asynchronous job execution

# Copyright (C) 2007 Philotic Inc.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require 'beanstalker/queue'

module Beanstalker; end

class Beanstalker::Worker

  SLEEP_TIME = 60 if !defined?(SLEEP_TIME) # rails loads this file twice

  class << self
    attr_accessor :finish
    attr_accessor :custom_error_handler
    attr_accessor :custom_timeout_handler
    attr_accessor :before_filter

    def error_handler(&block)
      self.custom_error_handler = block
    end

    def timeout_handler(&block)
      self.custom_timeout_handler = block
    end

    def before_reserves
      @before_reserves ||= []
    end

    def before_reserve(&block)
      before_reserves << block
    end

    def run_before_reserve
      before_reserves.each {|b| b.call}
    end
  end
  
  def initialize(top_binding, options = {})
    @top_binding = top_binding
    @stop = false
    @options = options
    if @options && @options[:servers]
      Beanstalker::Queue.queue = Beanstalk::Pool.new(@options[:servers])
    end
  end

  def main_loop
    trap('TERM') { @stop = true }
    loop do
      break if @stop
      safe_dispatch(get_job)
    end
  end

  def startup
    tubes = Array.wrap(@options[:tube] || "default").map(&:to_s) #["default"]
    watched_tubes = Beanstalker::Queue.queue.list_tubes_watched.values.flatten #["default"]
    to_watch = tubes - watched_tubes
    to_ignore = watched_tubes - tubes
    to_watch.each do |t|
      Beanstalker::Queue.queue.watch(t)
    end
    to_ignore.each do |t|
      Beanstalker::Queue.queue.ignore(t)
    end
    Daemonizer.logger.info "Using tubes: #{Beanstalker::Queue.queue.list_tubes_watched.values.flatten.join(',')}"
    Daemonizer.flush_logger
  end

  def shutdown
    do_all_work
  end

  def run
    startup
    main_loop
  rescue Interrupt
    shutdown
  end

  def q_hint
    @q_hint || Beanstalker::Queue.queue
  end
  
  def logger
    Daemonizer.logger or RAILS_DEFAULT_LOGGER
  end

  # This heuristic is to help prevent one queue from starving. The idea is that
  # if the connection returns a job right away, it probably has more available.
  # But if it takes time, then it's probably empty. So reuse the same
  # connection as long as it stays fast. Otherwise, have no preference.
  def reserve_and_set_hint
    t1 = Time.now.utc
    return job = q_hint.reserve
  ensure
    t2 = Time.now.utc
    @q_hint = if brief?(t1, t2) and job then job.conn else nil end
  end

  def brief?(t1, t2)
    ((t2 - t1) * 100).to_i.abs < 10
  end

  def get_job
    loop do
      begin
        Beanstalker::Queue.queue.connect
        self.class.run_before_reserve
        return reserve_and_set_hint
      rescue Interrupt => ex
        raise ex
      rescue SignalException => ex
        raise ex
      rescue Beanstalk::DeadlineSoonError
        # Do nothing; immediately try again, giving the user a chance to
        # clean up in the before_reserve hook.
        Daemonizer.logger.info 'Job deadline soon; you should clean up.'
      rescue Exception => ex
        @q_hint = nil # in case there's something wrong with this conn
        Daemonizer.logger.info(
          "#{ex.class}: #{ex}\n" + ex.backtrace.join("\n"))
        Daemonizer.logger.info 'something is wrong. We failed to get a job.'
        Daemonizer.logger.info "sleeping for #{SLEEP_TIME}s..."
        sleep(SLEEP_TIME)
      end
    end
  end

  def dispatch(job)
    ActiveRecord::Base.verify_active_connections!
    return run_ao_job(job) if beanstalker_job?(job)
  end

  def safe_dispatch(job)
    begin
      return dispatch(job)
    rescue Timeout::Error
      handle_timeout(job)
    rescue Interrupt => ex
      begin job.release rescue :ok end
      raise ex
    rescue Exception => ex
      handle_error(job, ex)
    ensure
      Daemonizer.flush_logger
    end
  end

  def handle_error(job, ex)
    if self.class.custom_error_handler
      self.class.custom_error_handler.call(job, ex)
    else
      self.class.default_handle_error(job, ex)
    end
  end

  def handle_timeout(job)
    if self.class.custom_timeout_handler
      self.class.custom_timeout_handler.call(job)
    else
      self.class.default_handle_timeout(job)
    end
  end

  def self.default_handle_error(job, ex)
    Daemonizer.logger.info "Job failed: #{job.server}/#{job.id}"
    Daemonizer.logger.info("#{ex.class}: #{ex}\n" + ex.backtrace.join("\n"))
    job.decay
  rescue Beanstalk::UnexpectedResponse => e
    Daemonizer.logger.info "Unexpected Beanstalkd error: #{job.server}/#{job.id}. #{e.inspect}"
  end

  def self.default_handle_timeout(job)
    Daemonizer.logger.info "Job timeout: #{job.server}/#{job.id}"
    job.decay
  rescue Beanstalk::UnexpectedResponse => e
    Daemonizer.logger.info "Unexpected Beanstalkd error: #{job.server}/#{job.id}. #{e.inspect}"
  end

  def run_ao_job(job)
    runner = lambda {
      f = self.class.before_filter
      result = f.call(job) if f
      run_code(job)
      job.delete
    }
    if @options[:ruby_timeout]
      timeout = (job.stats['ttr'].to_f * 0.8)
      logger.info "TO=#{timeout} sec. Job id=#{job.stats['id']}. Running '#{job[:code]}'. Age #{job.stats['age']}, Releases #{job.stats['releases']}, TTR #{job.stats['ttr']}"
      Timeout.timeout(timeout) do
        runner.call
      end
    else
      logger.info "Job id=#{job.stats['id']}. Running '#{job[:code]}'. Age #{job.stats['age']}, Releases #{job.stats['releases']}, TTR #{job.stats['ttr']}"
      runner.call
    end
    logger.info "Finished"
  end

  def run_code(job)
    eval(job.ybody[:code], @top_binding, "(beanstalk job #{job.id})", 1)
  end

  def beanstalker_job?(job)
    begin job.ybody[:type] == :rails rescue false end
  end

  def do_all_work
    Daemonizer.logger.info 'finishing all running jobs'
    f = self.class.finish
    f.call if f
  end
end

