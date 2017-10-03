# frozen_string_literal: true

module Sidekiq
  module Superworker
    module Server
      class Middleware
        def initialize(_options = nil)
          @processor = Sidekiq::Superworker::Processor.new
        end

        def call(worker, item, queue)
          begin
            return_value = yield
          rescue Sidekiq::Superworker::Exit => exception
            @processor.complete_all(item, exception)
            # This is to catch intentional exits from a SuperWorker
          rescue => exception
            @processor.error(worker, item, queue, exception)
            raise exception
          end
          @processor.complete(item)
          return_value
        end
      end
    end
  end
end
