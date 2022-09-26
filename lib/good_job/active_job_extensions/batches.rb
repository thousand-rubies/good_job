# frozen_string_literal: true
module GoodJob
  module ActiveJobExtensions
    module Batches
      extend ActiveSupport::Concern

      class_methods do
        def batch_enqueue(callback_job_class = nil, **properties, &block)
          GoodJob::Batch.enqueue(callback_job_class, **properties, &block)
        end
      end

      def batch
        CurrentThread.execution&.batch
      end
      alias batch? batch

      def batch_callback
        CurrentThread.execution&.batch_callback
      end
      alias batch_callback? batch_callback
    end
  end
end
