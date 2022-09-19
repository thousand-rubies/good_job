# frozen_string_literal: true
require 'active_support/core_ext/module/attribute_accessors_per_thread'

module GoodJob
  module Bulk
    thread_mattr_accessor :jobs

    def self.enqueue(wrap: nil)
      original_jobs = jobs
      self.jobs = []

      yield

      new_jobs = jobs
      self.jobs = nil

      enqueuer = lambda do
        new_jobs.each do |(adapter, active_job, scheduled_at)|
          adapter.enqueue_at(active_job, scheduled_at)
        end
      end

      wrap ? wrap.call(new_jobs, &enqueuer) : enqueuer.call
    ensure
      self.jobs = original_jobs
    end
  end
end
