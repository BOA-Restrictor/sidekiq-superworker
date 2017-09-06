# frozen_string_literal: true
module Sidekiq
  module Superworker
    class Processor
      def complete(item, _new_thread = true)
        Superworker.debug "JID ##{item['jid']}: Sidekiq job complete"

        complete_item(item)
      end

      def error(worker, item, _queue, exception)
        fail "Job has nil jid: #{item}" if item['jid'].nil?

        Superworker.debug "JID ##{item['jid']}: Error thrown"
        subjob = Subjob.find_by_jid(item['jid'])
        SubjobProcessor.error(subjob, worker, item, exception) if subjob
      end

      protected

      def complete_item(item)
        fail "Job has nil jid: #{item}" if item['jid'].nil?

        Superworker.debug "JID ##{item['jid']}: Passing job from Sidekiq to Superworker"
        subjob = Subjob.find_by_jid(item['jid'])
        SubjobProcessor.complete(subjob) if subjob
      end
    end
  end
end
