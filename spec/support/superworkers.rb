# frozen_string_literal: true
# Create dummy Sidekiq worker classes: Worker1..Worker9
(1..5).each do |i|
  class_name = "Worker#{i}"
  klass = Class.new do
    include Sidekiq::Worker

    sidekiq_options queue: 'sidekiq_superworker_test'

    def perform
      nil
    end
  end

  Object.const_set(class_name, klass)
end

# For testing worker exceptions
class FailingWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'sidekiq_superworker_test'

  def perform
    raise RuntimeError
  end
end

# For testing simple superworker properties
Sidekiq::Superworker::Worker.define(:SimpleSuperworker, :first_argument, :second_argument) do
  Worker1 :first_argument
  Worker2 :second_argument
end

# For testing complex blocks
Sidekiq::Superworker::Worker.define(:ComplexSuperworker, :first_argument, :second_argument) do
  Worker1 :first_argument do       # 1
    Worker2 :second_argument       # 2
    Worker3 :second_argument do    # 3
      Worker4 :first_argument      # 4
    end
    Worker5 :first_argument        # 5
  end
end

# For testing batch blocks
Sidekiq::Superworker::Worker.define(:BatchSuperworker, :user_ids) do
  batch user_ids: :user_id do
    Worker1 :user_id
    Worker2 :user_id
  end
end

# For testing empty arguments
Sidekiq::Superworker::Worker.define(:EmptyArgumentsSuperworker) do
  Worker1 do
    Worker2()
  end
end

# For testing nested superworkers
Sidekiq::Superworker::Worker.define(:ChildSuperworker) do
  Worker2 do
    Worker3()
  end
end
Sidekiq::Superworker::Worker.define(:NestedSuperworker) do
  Worker1()
  ChildSuperworker()
end

# For testing exceptions
Sidekiq::Superworker::Worker.define(:FailingSuperworker, :first_argument) do
  Worker1 :first_argument           # 1
  Worker2 :first_argument           # 2
end
