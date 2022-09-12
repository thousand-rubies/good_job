# frozen_string_literal: true
require 'rails_helper'

RSpec.describe 'Batches' do
  let(:adapter) { GoodJob::Adapter.new(execution_mode: :external) }

  before do
    ActiveJob::Base.queue_adapter = adapter
    GoodJob.preserve_job_records = true

    stub_const 'TestJob', (Class.new(ActiveJob::Base) do
      def self.after_batch(batch)
        # stubbed
      end

      ExpectedError = Class.new(StandardError)
      retry_on(ExpectedError, wait: 0, attempts: 1, jitter: 0) { nil }

      def perform(error: false)
        raise ExpectedError if error
      end
    end)

    stub_const 'BatchCallbackJob', (Class.new(ActiveJob::Base) do
      def perform(batch)
        # nil
      end
    end)

    describe 'simple batching' do
      it 'assigns a batch_id to all jobs in the batch' do
        active_job = nil
        batch = GoodJob::Batch.enqueue do
          active_job = TestJob.perform_later
        end

        good_job = GoodJob::Job.find_by(active_job_id: active_job.job_id)
        expect(good_job.batch_id).to eq batch.id
      end

      context 'when all jobs complete successfully' do
        it 'has success status' do
          batch = GoodJob::Batch.enqueue do
            TestJob.perform_later
          end

          expect(batch.completed_at).to be_within(1.second).of(Time.current)
          expect(batch).to be_enqueued

          GoodJob.perform_inline

          expect(batch).to be_complete
          expect(batch).to be_successful

          expect(batch.completed_at).to be_within(1.second).of(Time.current)
          expect(batch.failed_at).to be_nil
        end
      end

      it 'when a job is discarded' do
        it "has a failure status" do
          batch = GoodJob::Batch.enqueue do
            TestJob.perform_later(error: true)
          end

          GoodJob.perform_inline

          expect(batch).to be_completed
          expect(batch).to be_failed
          expect(batch.completed_at).to be_within(1.second).of(Time.current)
          expect(batch.failed_at).to be_within(1.second).of(Time.current)
        end
      end

      context 'when there is a callback' do
        it 'calls the callback with a batch' do
          batch = GoodJob::Batch.enqueue(callback_job: "BatchCallbackJob", some_property: "foobar") do
            TestJob.perform_later
          end

          expect(batch.properties).to eq({ some_property: "foobar" })

          GoodJob.perform_inline

          expect(TestJob).to have_received(:after_batch).with(batch)

          last_job = GoodJob::Job.last
          expect(last_job).to have_attributes(job_class: 'BatchCallbackJob')
        end
      end
    end

    describe 'complex batching' do
      it 'can be used as instance object' do
        batch = GoodJob::Batch.new
        batch.callback_job = "BatchCallbackJob"
        batch.callback_queue = "custom_queue"
        batch.callback_priority = 10

        expect(batch.id).to be_present
        expect(batch).not_to be_persisted
        expect(batch).not_to be_enqueued

        # keep adding jobs to the batch
        batch.add do
          TestJob.perform_later
        end

        expect(batch).not_to be_enqueued

        # it's ok for the jobs to already be run; this is heavily asynchronous
        GoodJob.perform_inline

        batch.enqueue
        expect(batch.enqueued).to be_within(1.second).of(Time.current)

        GoodJob.perform_inline(queue: "custom_queue") # for the callback job
        expect(TestJob).to have_received(:after_batch).with(batch)
        expect(GoodJob::Job.last).to have_attributes(job_class: 'GoodJob::Batch::CallbackJob', priority: 10, queue: "custom_queue")
      end
    end
  end
