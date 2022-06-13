module GoodJob
  # A manager holds all thread scheduling instances
  # (Scheduler, Notifier, Poller, ConManager) for the current process
  class Manager
    attr_reader :scheduler

    # @param configuration [Configuration]
    def initialize(configuration)
      @configuration = configuration
    end

    def execute(job_state)
      start if @scheduler.nil?
      @scheduler.create_thread(job_state)
    end

    def start
      @notifier = GoodJob::Notifier.new
      @poller = GoodJob::Poller.new(poll_interval: @configuration.poll_interval)
      @scheduler = GoodJob::Scheduler.from_configuration(@configuration, warm_cache_on_initialize: true)
      @notifier.recipients << [@scheduler, :create_thread]
      @poller.recipients << [@scheduler, :create_thread]

      @cron_manager = GoodJob::CronManager.new(@configuration.cron_entries, start_on_initialize: true) if @configuration.enable_cron?
    end

    # @param timeout [Numeric, nil] Seconds to wait for active threads to finish.
    # @return [void]
    def restart(timeout: -1)
      GoodJob._shutdown_all(executables, :restart, timeout: timeout)
    end

    # @param timeout [nil, Numeric] Seconds to wait for actively executing jobs to finish
    #   * +nil+, the scheduler will trigger a shutdown but not wait for it to complete.
    #   * +-1+, the scheduler will wait until the shutdown is complete.
    #   * +0+, the scheduler will immediately shutdown and stop any active tasks.
    #   * +1..+, the scheduler will wait that many seconds before stopping any remaining active tasks.
    # @param wait [Boolean] whether to wait for shutdown
    # @return [void]
    def shutdown(timeout: -1)
      GoodJob._shutdown_all(executables, timeout: timeout)
    end

    # @return [Boolean] whether background threads are shut down
    def shutdown?
      executables.all?(&:shutdown?)
    end

    def scheduler_shutdown?
      @scheduler.nil? ? true : @scheduler.shutdown?
    end

    def notifier_shutdown?
      @notifier.nil? ? true : @notifier.shutdown?
    end

    private

    def executables
      [@poller, @scheduler, @notifier, @cron_manager].compact
    end
  end
end
