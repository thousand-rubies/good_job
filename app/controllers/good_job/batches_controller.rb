# frozen_string_literal: true
module GoodJob
  class BatchesController < GoodJob::ApplicationController
    def index
      @filter = BatchesFilter.new(params)
    end
  end
end
