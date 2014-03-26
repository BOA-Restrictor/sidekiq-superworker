module Sidekiq
  module Superworker
    class WorkerClass
      class << self
        attr_reader :nested_hash

        def is_a_superworker?
          true
        end

        def perform_async(*arg_values)
          options = initialize_superjob(arg_values)
          subjobs = create_subjobs(arg_values, options)
          SuperjobProcessor.create(@superjob_id, @class_name, arg_values, subjobs, options)
        end

        protected

        def initialize_superjob(arg_values)
          options = {}
          
          # If an additional argument value is given, it's the superjob's options
          if (arg_values.length == @arg_keys.length + 1) && arg_values.last.is_a?(Hash)
            options = arg_values.last
          elsif @arg_keys.length != arg_values.length
            raise "Wrong number of arguments for #{name}.perform_async (#{arg_values.length} for #{@arg_keys.length})"
          end

          @args = Hash[@arg_keys.zip(arg_values)]
          @superjob_id = SecureRandom.hex(12)

          options
        end

        def create_subjobs(arg_values, options={})
          records = DSLHash.new(@nested_hash, @args).to_records
          records = records.collect do |id, record|
            record[:status] = 'initialized'
            record[:superjob_id] = @superjob_id
            record[:superworker_class] = @class_name
            record[:meta] = options[:meta] if options[:meta]
            unless record.key?(:arg_values)
              record[:arg_values] = record[:arg_keys].collect do |arg_key|
                # Allow for subjob arg_values to be set within the superworker definition; if a symbol is
                # used in the DSL, use @args[arg_key], and otherwise use arg_key as the value
                arg_key.is_a?(Symbol) ? @args[arg_key] : arg_key
              end
            end
            record
          end

          insert_subjobs(records)
        end

        def insert_subjobs(records)
          if Sidekiq::Superworker.options[:insert_method] == :multiple
            subjobs = records.collect { |record| Sidekiq::Superworker::Subjob.new(record) }
            Sidekiq::Superworker::Subjob.import(subjobs)
            Sidekiq::Superworker::Subjob.where(superjob_id: @superjob_id).order(:subjob_id)
          else
            Sidekiq::Superworker::Subjob.transaction do
              Sidekiq::Superworker::Subjob.create(records)
            end
          end
        end
      end
    end
  end
end
