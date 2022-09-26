# frozen_string_literal: true
require "active_job"
require "active_job/queue_adapters"

require "good_job/version"
require "good_job/engine"

require "good_job/adapter"
require "active_job/queue_adapters/good_job_adapter"
require "good_job/active_job_extensions/concurrency"
require "good_job/active_job_extensions/batches"

require "good_job/assignable_connection"
require "good_job/bulk"
require "good_job/cleanup_tracker"
require "good_job/cli"
require "good_job/configuration"
require "good_job/cron_manager"
require 'good_job/current_thread'
require "good_job/daemon"
require "good_job/dependencies"
require "good_job/job_performer"
require "good_job/log_subscriber"
require "good_job/multi_scheduler"
require "good_job/notifier"
require "good_job/poller"
require "good_job/probe_server"
require "good_job/scheduler"

# GoodJob is a multithreaded, Postgres-based, ActiveJob backend for Ruby on Rails.
#
# +GoodJob+ is the top-level namespace and exposes configuration attributes.
module GoodJob
  include GoodJob::Dependencies

  DEFAULT_LOGGER = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new($stdout))

  # @!attribute [rw] active_record_parent_class
  #   @!scope class
  #   The ActiveRecord parent class inherited by +GoodJob::Execution+ (default: +ActiveRecord::Base+).
  #   Use this when using multiple databases or other custom ActiveRecord configuration.
  #   @return [ActiveRecord::Base]
  #   @example Change the base class:
  #     GoodJob.active_record_parent_class = "CustomApplicationRecord"
  mattr_accessor :active_record_parent_class, default: "ActiveRecord::Base"

  # @!attribute [rw] logger
  #   @!scope class
  #   The logger used by GoodJob (default: +Rails.logger+).
  #   Use this to redirect logs to a special location or file.
  #   @return [Logger, nil]
  #   @example Output GoodJob logs to a file:
  #     GoodJob.logger = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new("log/my_logs.log"))
  mattr_accessor :logger, default: DEFAULT_LOGGER

  # @!attribute [rw] preserve_job_records
  #   @!scope class
  #   Whether to preserve job records in the database after they have finished (default: +true+).
  #   By default, GoodJob deletes job records after the job is completed successfully.
  #   If you want to preserve jobs for latter inspection, set this to +true+.
  #   If you want to preserve only jobs that finished with error for latter inspection, set this to +:on_unhandled_error+.
  #   @return [Boolean, nil]
  mattr_accessor :preserve_job_records, default: true

  # @!attribute [rw] retry_on_unhandled_error
  #   @!scope class
  #   Whether to re-perform a job when a type of +StandardError+ is raised to GoodJob (default: +false+).
  #   If +true+, causes jobs to be re-queued and retried if they raise an instance of +StandardError+.
  #   If +false+, jobs will be discarded or marked as finished if they raise an instance of +StandardError+.
  #   Instances of +Exception+, like +SIGINT+, will *always* be retried, regardless of this attribute's value.
  #   @return [Boolean, nil]
  mattr_accessor :retry_on_unhandled_error, default: false

  # @!attribute [rw] on_thread_error
  #   @!scope class
  #   This callable will be called when an exception reaches GoodJob (default: +nil+).
  #   It can be useful for logging errors to bug tracking services, like Sentry or Airbrake.
  #   @example Send errors to Sentry
  #     # config/initializers/good_job.rb
  #     GoodJob.on_thread_error = -> (exception) { Raven.capture_exception(exception) }
  #   @return [Proc, nil]
  mattr_accessor :on_thread_error, default: nil

  # @!attribute [rw] configuration
  #   @!scope class
  #   Global configuration object for GoodJob.
  #   @return [GoodJob::Configuration, nil]
  mattr_accessor :configuration, default: GoodJob::Configuration.new({})

  # Called with exception when a GoodJob thread raises an exception
  # @param exception [Exception] Exception that was raised
  # @return [void]
  def self._on_thread_error(exception)
    on_thread_error.call(exception) if on_thread_error.respond_to?(:call)
  end

  # Stop executing jobs.
  # GoodJob does its work in pools of background threads.
  # When forking processes you should shut down these background threads before forking, and restart them after forking.
  # For example, you should use +shutdown+ and +restart+ when using async execution mode with Puma.
  # See the {file:README.md#executing-jobs-async--in-process} for more explanation and examples.
  # @param timeout [nil, Numeric] Seconds to wait for actively executing jobs to finish
  #   * +nil+, the scheduler will trigger a shutdown but not wait for it to complete.
  #   * +-1+, the scheduler will wait until the shutdown is complete.
  #   * +0+, the scheduler will immediately shutdown and stop any active tasks.
  #   * +1..+, the scheduler will wait that many seconds before stopping any remaining active tasks.
  # @param wait [Boolean] whether to wait for shutdown
  # @return [void]
  def self.shutdown(timeout: -1)
    _shutdown_all(_executables, timeout: timeout)
  end

  # Tests whether jobs have stopped executing.
  # @return [Boolean] whether background threads are shut down
  def self.shutdown?
    _executables.all?(&:shutdown?)
  end

  # Stops and restarts executing jobs.
  # GoodJob does its work in pools of background threads.
  # When forking processes you should shut down these background threads before forking, and restart them after forking.
  # For example, you should use +shutdown+ and +restart+ when using async execution mode with Puma.
  # See the {file:README.md#executing-jobs-async--in-process} for more explanation and examples.
  # @param timeout [Numeric, nil] Seconds to wait for active threads to finish.
  # @return [void]
  def self.restart(timeout: -1)
    _shutdown_all(_executables, :restart, timeout: timeout)
  end

  # Sends +#shutdown+ or +#restart+ to executable objects ({GoodJob::Notifier}, {GoodJob::Poller}, {GoodJob::Scheduler}, {GoodJob::MultiScheduler}, {GoodJob::CronManager})
  # @param executables [Array<Notifier, Poller, Scheduler, MultiScheduler, CronManager>] Objects to shut down.
  # @param method_name [:symbol] Method to call, e.g. +:shutdown+ or +:restart+.
  # @param timeout [nil,Numeric]
  # @return [void]
  def self._shutdown_all(executables, method_name = :shutdown, timeout: -1)
    if timeout.is_a?(Numeric) && timeout.positive?
      executables.each { |executable| executable.send(method_name, timeout: nil) }

      stop_at = Time.current + timeout
      executables.each { |executable| executable.send(method_name, timeout: [stop_at - Time.current, 0].max) }
    else
      executables.each { |executable| executable.send(method_name, timeout: timeout) }
    end
  end

  # Destroys preserved job records.
  # By default, GoodJob destroys job records when the job is performed and this
  # method is not necessary. However, when `GoodJob.preserve_job_records = true`,
  # the jobs will be preserved in the database. This is useful when wanting to
  # analyze or inspect job performance.
  # If you are preserving job records this way, use this method regularly to
  # destroy old records and preserve space in your database.
  # @params older_than [nil,Numeric,ActiveSupport::Duration] Jobs older than this will be destroyed (default: +86400+).
  # @return [Integer] Number of jobs that were destroyed.
  def self.cleanup_preserved_jobs(older_than: nil)
    older_than ||= GoodJob.configuration.cleanup_preserved_jobs_before_seconds_ago
    timestamp = Time.current - older_than
    include_discarded = GoodJob.configuration.cleanup_discarded_jobs?

    ActiveSupport::Notifications.instrument("cleanup_preserved_jobs.good_job", { older_than: older_than, timestamp: timestamp }) do |payload|
      old_jobs = GoodJob::Job.where('finished_at <= ?', timestamp)
      old_jobs = old_jobs.not_discarded unless include_discarded
      old_jobs_count = old_jobs.count

      GoodJob::Execution.where(job: old_jobs).delete_all
      payload[:destroyed_records_count] = old_jobs_count
    end
  end

  # Perform all queued jobs in the current thread.
  # This is primarily intended for usage in a test environment.
  # Unhandled job errors will be raised.
  # @param queue_string [String] Queues to execute jobs from
  # @return [void]
  def self.perform_inline(queue_string = "*")
    job_performer = JobPerformer.new(queue_string)
    loop do
      result = job_performer.next
      break unless result
      raise result.unhandled_error if result.unhandled_error
    end
  end

  def self._executables
    [].concat(
      CronManager.instances,
      Notifier.instances,
      Poller.instances,
      Scheduler.instances
    )
  end

  ActiveSupport.run_load_hooks(:good_job, self)
end
