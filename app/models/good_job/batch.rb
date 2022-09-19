# frozen_string_literal: true

module GoodJob
  class Batch < BaseRecord
    include Lockable

    thread_mattr_accessor :current_batch_id
    thread_mattr_accessor :current_batch_callback_id

    self.table_name = 'good_job_batches'

    has_many :executions, class_name: 'GoodJob::Execution', inverse_of: :batch, dependent: nil
    has_many :jobs, class_name: 'GoodJob::Job', inverse_of: :batch, dependent: nil

    alias_attribute :enqueued?, :enqueued_at
    alias_attribute :completed?, :completed_at
    alias_attribute :failed?, :failed_at

    PROTECTED_PARAMS = %i[
      callback_job_class
      callback_queue_name
      callback_priority
    ].freeze

    def self.enqueue(_callback_job_class = nil, **params, &block)
      new_params = params.dup
      batch_attrs = PROTECTED_PARAMS.index_with { |key| new_params.delete(key) }
      batch_attrs[:params] = new_params

      new(batch_attrs).tap do |batch|
        batch.enqueue(&block)
      end
    end

    def succeeded?
      !failed? && completed?
    end

    def add(&block)
      save

      wrapper = lambda do |_jobs|
        self.class.current_batch_id = id
        yield
      ensure
        self.class.current_batch_id = nil
      end

      Bulk.enqueue(wrap: wrapper, &block)
    end

    def enqueue(&block)
      add(&block) if block
      self.enqueued_at = Time.current if enqueued_at.nil?
      save
      _finalize
    end

    def params=(value)
      @_params = value
      self.serialized_params = ActiveJob::Arguments.serialize(value)
    end

    def params
      @_params ||= ActiveJob::Arguments.deserialize(serialized_params)
    end

    def _finalize(execution = nil)
      execution_discarded = execution && execution.error.present? && execution.retried_good_job_id.nil?
      with_advisory_lock(function: "pg_advisory_lock") do
        update(failed_at: Time.current) if execution_discarded && failed_at.blank?

        if jobs.where(finished_at: nil).count.zero?
          update(completed_at: Time.current)
          return if callback_job_class.blank?

          callback_job_klass = callback_job_class.constantize

          begin
            original_current_batch_callback_id = self.class.current_batch_callback_id
            self.class.current_batch_callback_id = id

            callback_job_klass.set(priority: callback_priority, queue: callback_queue_name).perform_later(self)
          ensure
            self.class.current_batch_callback_id = original_current_batch_callback_id
          end
        end
      end
    end
  end
end
