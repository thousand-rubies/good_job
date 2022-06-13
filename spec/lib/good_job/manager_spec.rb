# frozen_string_literal: true
require 'rails_helper'

RSpec.describe GoodJob::Manager do
  let(:configuration) { GoodJob::Configuration.new({ enable_cron: true, probe_port: 6000 }) }

  describe '#start' do
    it 'initializes scheduler and other objects' do
      manager = described_class.new(configuration)
      manager.start

      executable_classes = [
        GoodJob::Scheduler,
        GoodJob::Notifier,
        GoodJob::Poller,
        GoodJob::CronManager,
        GoodJob::ProbeServer
      ]

      executable_classes.each do |executable_class|
        expect(executable_class.instances.size).to eq 1
        instance = executable_class.instances.first
        wait_until { expect(instance).to be_running }
      end

      manager.shutdown
      expect(manager).to be_shutdown

      executable_classes.each do |executable_class|
        instance = executable_class.instances.first
        expect(instance).to be_shutdown
      end
    end
  end
end
