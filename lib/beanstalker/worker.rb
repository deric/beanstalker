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
require 'beanstalker/mapper'

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
    mapper_file = "#{RAILS_ROOT}/config/beanstalker_mapper.rb"
    @mapper = Beanstalker::Mapper.new(mapper_file) if File.exist?(mapper_file)
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
    logger.info "Got job: #{get_job_body(job).inspect}"
    if rails_job?(job)
      run_ao_job(job)
    elsif mapped_job?(job)
      run_mapped_job(job)
    else
      logger.error "Job #{job.inspect} cannot be processed... deleteing"
      job.delete
    end
  rescue Exception => e
    logger.error "Exception: #{e.inspect}... Bury job"
    job.bury
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

  def class_error_handler(klass)
    if klass.respond_to?(:async_error_handler) and
       async_error_handler = klass.async_error_handler and
       async_error_handler.is_a?(Proc)
    then
      async_error_handler
    else
      false
    end
  end

  def handle_error(job, ex)
    custom_error_handler_ok = false
    Daemonizer.logger.warn "Handling exception: #{ex.backtrace.join('\n')}, job = #{job.id}"

    if rails_job?(job)
      if job[:class]
        klass = Object.const_get(job[:class])
        error_handler = class_error_handler(klass)
        if error_handler.is_a?(Proc)
          Daemonizer.logger.info "Running custom error handler for class #{job[:class]}, job = #{job.id}"
          error_handler.call(job, ex)
          job_reserved = begin
            job.stats['state'] == 'reserved'
          rescue Beanstalk::NotFoundError
            false
          end
          if job_reserved
            Daemonizer.logger.info "Custom error handler for class #{job[:class]} didn't release job. job = #{job.id}"
          else
            Daemonizer.logger.info "Custom error handler for class #{job[:class]} released job. job = #{job.id}"
            custom_error_handler_ok = true
          end
        end
      end
    end

    unless custom_error_handler_ok
      Daemonizer.logger.info "Running common handler. job = #{job.id}"
      if self.class.custom_error_handler
        self.class.custom_error_handler.call(job, ex)
      else
        self.class.default_handle_error(job, ex)
      end
    else
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

  def run_with_ruby_timeout_if_set(job_desc, job, &block)
    if @options[:ruby_timeout]
      timeout = (job.stats['ttr'].to_f * 0.8)
      logger.info "TO=#{timeout} sec. Job id=#{job.stats['id']}. Running '#{job_desc}'. Age #{job.stats['age']}, Releases #{job.stats['releases']}, TTR #{job.stats['ttr']}"
      Timeout.timeout(timeout) do
        block.call
      end
    else
      logger.info "Job id=#{job.stats['id']}. Running '#{job_desc}'. Age #{job.stats['age']}, Releases #{job.stats['releases']}, TTR #{job.stats['ttr']}"
      block.call
    end
  end

  def run_mapped_job(job)
    job_body = get_job_body(job)
    job_kind = job_body['kind']
    job_data = job_body['data']
    job_method = job_data['method']

    job_desc = "#{job_kind}/#{job_method}"

    run_with_ruby_timeout_if_set(job_desc, job) do
      t1 = Time.now
      @map_job = @mapper && @mapper.method_for(job_kind, job_method)
      if @map_job
        @map_job.call(job_data['body'] || {})
        logger.info "Finished. Job id=#{job.stats['id']}. Mapped from '#{job_desc}'. Time taken: #{(Time.now - t1).to_f} sec"
      else
        logger.error "Job id=#{job.stats['id']}. Mapping not found: '#{job_desc}'. Releases #{job.stats['releases']}. Deleting"
      end
      job.delete
    end
  end

  def get_job_body(job)
    job.ybody.with_indifferent_access
  end

  def run_ao_job(job)
    job_data = get_job_body(job)['data']
    code = job_data['code']
    run_with_ruby_timeout_if_set(code, job) do
      t1 = Time.now
      f = self.class.before_filter
      statistics = job.stats.dup
      can_run = f ? f.call(job) : true
      if can_run
        run_code(job.id, code)
        job.delete
        logger.info "Finished. Job id=#{statistics['id']}. Code '#{code}'. Time taken: #{(Time.now - t1).to_f} sec"
      else
        logger.info "Not runnind due to :before_filter restriction. Job id=#{statistics['id']}. Code '#{code}'."
      end
    end
  end

  def run_code(job_id, code)
    eval(code, @top_binding, "(beanstalk job #{job_id})", 1)
  end

  def rails_job?(job)
    get_job_body(job)['kind'].to_s == 'rails_beanstalker'
  end

  def mapped_job?(job)
    @mapper && @mapper.can_handle_kind?(get_job_body(job)['kind'])
  end

  def do_all_work
    logger.info 'finishing all running jobs'
    f = self.class.finish
    f.call if f
  end
end

