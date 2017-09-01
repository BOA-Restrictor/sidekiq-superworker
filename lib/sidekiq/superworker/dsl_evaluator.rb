# frozen_string_literal: true
module Sidekiq
  module Superworker
    class DSLEvaluator
      def method_missing(method, *args, &block)
        Fiber.yield([method, args, block])
      end
    end
  end
end
