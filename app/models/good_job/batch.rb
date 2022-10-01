# frozen_string_literal: true

module GoodJob
  class Batch < BaseRecord
    include Lockable

    thread_mattr_accessor :current_batch_id
    thread_mattr_accessor :current_batch_callback_id

    self.table_name = 'good_job_batches'

    has_many :executions, class_name: 'GoodJob::Execution', inverse_of: :batch, dependent: nil
    has_many :jobs, class_name: 'GoodJob::Job', inverse_of: :batch, dependent: nil
    has_many :callback_jobs, class_name: 'GoodJob::Job', foreign_key: :batch_callback_id, dependent: nil # rubocop:disable Rails/InverseOf

    scope :finished, -> { where.not(finished_at: nil) }
    scope :discarded, -> { where.not(discarded_at: nil) }
    scope :not_discarded, -> { where(discarded_at: nil) }
    scope :succeeded, -> { finished.not_discarded}

    alias_attribute :enqueued?, :enqueued_at
    alias_attribute :discarded?, :discarded_at
    alias_attribute :finished?, :finished_at

    PROTECTED_PROPERTIES = %i[
      description
      callback_job_class
      callback_queue_name
      callback_priority
      description
    ].freeze

    scope :display_all, (lambda do |after_created_at: nil, after_id: nil|
      query = order(created_at: :desc, id: :desc)
      if after_created_at.present? && after_id.present?
        query = query.where(Arel.sql('(created_at, id) < (:after_created_at, :after_id)'), after_created_at: after_created_at, after_id: after_id)
      elsif after_created_at.present?
        query = query.where(Arel.sql('(after_created_at) < (:after_created_at)'), after_created_at: after_created_at)
      end
      query
    end)

    def self.enqueue(callback_job_class = nil, **properties, &block)
      new.tap do |batch|
        batch.enqueue(callback_job_class, **properties, &block)
      end
    end

    def self.within_thread(batch_id: nil, batch_callback_id: nil)
      original_batch_id = current_batch_id
      original_batch_callback_id = current_batch_callback_id

      self.current_batch_id = batch_id
      self.current_batch_callback_id = batch_callback_id

      yield
    ensure
      self.current_batch_id = original_batch_id
      self.current_batch_callback_id = original_batch_callback_id
    end

    def succeeded?
      !discarded? && finished?
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

    def enqueue(callback_job_class = nil, **properties, &block)
      properties = properties.dup
      batch_attrs = PROTECTED_PROPERTIES.index_with { |key| properties.delete(key) }.compact
      batch_attrs[:callback_job_class] = callback_job_class if callback_job_class
      batch_attrs[:properties] = self.properties.merge(properties)

      update(batch_attrs)
      add(&block) if block
      self.enqueued_at = Time.current if enqueued_at.nil?
      save
      _continue_discard_or_finish
    end

    def properties=(value)
      self.serialized_properties = ActiveJob::Arguments.serialize([value])
    end

    def properties
      return {} if serialized_properties.blank?

      ActiveJob::Arguments.deserialize(serialized_properties).first
    end

    def display_attributes
      attributes.except('serialized_properties').merge(properties: properties)
    end

    def _continue_discard_or_finish(execution = nil)
      execution_discarded = execution && execution.error.present? && execution.retried_good_job_id.nil?
      with_advisory_lock(function: "pg_advisory_lock") do
        update(discarded_at: Time.current) if execution_discarded && discarded_at.blank?

        if enqueued_at && jobs.where(finished_at: nil).count.zero?
          update(finished_at: Time.current)
          return if callback_job_class.blank?

          callback_job_klass = callback_job_class.constantize
          self.class.within_thread(batch_id: nil, batch_callback_id: id) do
            callback_job_klass.set(priority: callback_priority, queue: callback_queue_name).perform_later(self)
          end
        end
      end
    end
  end
end
