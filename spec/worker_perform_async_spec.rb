# frozen_string_literal: true
require 'spec_helper'

describe Sidekiq::Superworker::Worker do
  include Sidekiq::Superworker::WorkerHelpers

  before :all do
    @queue = Sidekiq::Queue.new(dummy_worker_queue)
    clean_datastores
  end

  describe '.perform_async' do
    context 'batch superworker' do
      before :all do
        BatchSuperworker.perform_async(user_ids: [100, 101])
      end

      after :all do
        clean_datastores
      end

      it 'creates the correct Subjob records' do
        expected_record_hashes =
          { 1 =>
            { subjob_id: 1,
              parent_id: nil,
              children_ids: [2, 5],
              next_id: nil,
              subworker_class: 'batch',
              superworker_class: 'BatchSuperworker',
              arg_keys: [{ 'user_ids' => 'user_id' }],
              arg_values: [{ 'user_ids' => 'user_id' }],
              status: 'running',
              descendants_are_complete: false },
            2 =>
            { subjob_id: 2,
              parent_id: 1,
              children_ids: [3, 4],
              next_id: nil,
              subworker_class: 'batch_child',
              superworker_class: 'BatchSuperworker',
              arg_keys: ['user_id'],
              arg_values: [100],
              status: 'running',
              descendants_are_complete: false },
            3 =>
            { subjob_id: 3,
              parent_id: 2,
              children_ids: nil,
              next_id: 4,
              subworker_class: 'Worker1',
              superworker_class: 'BatchSuperworker',
              arg_keys: ['user_id'],
              arg_values: [100],
              status: 'queued',
              descendants_are_complete: false },
            4 =>
            { subjob_id: 4,
              parent_id: 2,
              children_ids: nil,
              next_id: nil,
              subworker_class: 'Worker2',
              superworker_class: 'BatchSuperworker',
              arg_keys: ['user_id'],
              arg_values: [100],
              status: 'initialized',
              descendants_are_complete: false },
            5 =>
            { subjob_id: 5,
              parent_id: 1,
              children_ids: [6, 7],
              next_id: nil,
              subworker_class: 'batch_child',
              superworker_class: 'BatchSuperworker',
              arg_keys: ['user_id'],
              arg_values: [101],
              status: 'running',
              descendants_are_complete: false },
            6 =>
            { subjob_id: 6,
              parent_id: 5,
              children_ids: nil,
              next_id: 7,
              subworker_class: 'Worker1',
              superworker_class: 'BatchSuperworker',
              arg_keys: ['user_id'],
              arg_values: [101],
              status: 'queued',
              descendants_are_complete: false },
            7 =>
            { subjob_id: 7,
              parent_id: 5,
              children_ids: nil,
              next_id: nil,
              subworker_class: 'Worker2',
              superworker_class: 'BatchSuperworker',
              arg_keys: ['user_id'],
              arg_values: [101],
              status: 'initialized',
              descendants_are_complete: false } }

        record_hashes = subjobs_to_indexed_hash(Sidekiq::Superworker::Subjob.all)
        record_hashes.length.should eq(expected_record_hashes.length)
        record_hashes.each do |subjob_id, record_hash|
          expected_record_hashes[subjob_id].should == record_hash
        end
      end
    end

    context 'empty arguments superworker' do
      before :all do
        EmptyArgumentsSuperworker.perform_async
      end

      after :all do
        clean_datastores
      end

      it 'creates the correct Subjob records' do
        expected_record_hashes = {
          1 =>
           { subjob_id: 1,
             parent_id: nil,
             children_ids: [2],
             next_id: nil,
             subworker_class: 'Worker1',
             superworker_class: 'EmptyArgumentsSuperworker',
             arg_keys: [],
             arg_values: [],
             status: 'queued',
             descendants_are_complete: false },
          2 =>
          { subjob_id: 2,
            parent_id: 1,
            children_ids: nil,
            next_id: nil,
            subworker_class: 'Worker2',
            superworker_class: 'EmptyArgumentsSuperworker',
            arg_keys: [],
            arg_values: [],
            status: 'initialized',
            descendants_are_complete: false }
        }
        record_hashes = subjobs_to_indexed_hash(Sidekiq::Superworker::Subjob.all)

        record_hashes.length.should eq(expected_record_hashes.length)
        record_hashes.each do |subjob_id, record_hash|
          expected_record_hashes[subjob_id].should == record_hash
        end
      end
    end

    context 'nested superworker' do
      before :all do
        NestedSuperworker.perform_async
      end

      after :all do
        clean_datastores
      end

      it 'creates the correct Subjob records' do
        expected_record_hashes = {
          1 =>
            { subjob_id: 1,
              parent_id: nil,
              children_ids: nil,
              next_id: 2,
              subworker_class: 'Worker1',
              superworker_class: 'NestedSuperworker',
              arg_keys: [],
              arg_values: [],
              status: 'queued',
              descendants_are_complete: false },
          2 =>
            { subjob_id: 2,
              parent_id: nil,
              children_ids: [3],
              next_id: nil,
              subworker_class: 'ChildSuperworker',
              superworker_class: 'NestedSuperworker',
              arg_keys: [],
              arg_values: [],
              status: 'initialized',
              descendants_are_complete: false },
          3 =>
            { subjob_id: 3,
              parent_id: 2,
              children_ids: [4],
              next_id: nil,
              subworker_class: 'Worker2',
              superworker_class: 'NestedSuperworker',
              arg_keys: [],
              arg_values: [],
              status: 'initialized',
              descendants_are_complete: false },
          4 =>
            { subjob_id: 4,
              parent_id: 3,
              children_ids: nil,
              next_id: nil,
              subworker_class: 'Worker3',
              superworker_class: 'NestedSuperworker',
              arg_keys: [],
              arg_values: [],
              status: 'initialized',
              descendants_are_complete: false }
        }

        record_hashes = subjobs_to_indexed_hash(Sidekiq::Superworker::Subjob.all)

        record_hashes.length.should eq(expected_record_hashes.length)
        record_hashes.each do |subjob_id, record_hash|
          expected_record_hashes[subjob_id].should == record_hash
        end
      end
    end

    context 'complex superworker' do
      before :all do
        @superjob_id = worker_perform_async(ComplexSuperworker)
      end

      after :all do
        clean_datastores
      end

      it 'creates the correct Subjob records' do
        expected_record_hashes = {
          1 =>
           { subjob_id: 1,
             parent_id: nil,
             children_ids: [2, 3, 5],
             next_id: nil,
             subworker_class: 'Worker1',
             superworker_class: 'ComplexSuperworker',
             arg_keys: ['first_argument'],
             arg_values: [100],
             status: 'queued',
             descendants_are_complete: false },
          2 =>
          { subjob_id: 2,
            parent_id: 1,
            children_ids: nil,
            next_id: 3,
            subworker_class: 'Worker2',
            superworker_class: 'ComplexSuperworker',
            arg_keys: ['second_argument'],
            arg_values: [101],
            status: 'initialized',
            descendants_are_complete: false },
          3 =>
          { subjob_id: 3,
            parent_id: 1,
            children_ids: [4],
            next_id: 5,
            subworker_class: 'Worker3',
            superworker_class: 'ComplexSuperworker',
            arg_keys: ['second_argument'],
            arg_values: [101],
            status: 'initialized',
            descendants_are_complete: false },
          4 =>
          { subjob_id: 4,
            parent_id: 3,
            children_ids: nil,
            next_id: nil,
            subworker_class: 'Worker4',
            superworker_class: 'ComplexSuperworker',
            arg_keys: ['first_argument'],
            arg_values: [100],
            status: 'initialized',
            descendants_are_complete: false },
          5 =>
          { subjob_id: 5,
            parent_id: 1,
            children_ids: nil,
            next_id: nil,
            subworker_class: 'Worker5',
            superworker_class: 'ComplexSuperworker',
            arg_keys: ['first_argument'],
            arg_values: [100],
            status: 'initialized',
            descendants_are_complete: false }
        }

        record_hashes = subjobs_to_indexed_hash(Sidekiq::Superworker::Subjob.all)

        record_hashes.length.should eq(expected_record_hashes.length)
        record_hashes.each do |subjob_id, record_hash|
          expected_record_hashes[subjob_id].should == record_hash
        end
      end

      it 'creates enough Subjob records' do
        Sidekiq::Superworker::Subjob.count.should == 5
      end

      it 'queues root-level subjobs' do
        jid = Sidekiq::Superworker::Subjob.jid(@superjob_id, '1')
        Sidekiq::Superworker::Subjob.find_by_jid(jid).status.should == 'queued'
      end

      it 'creates a Sidekiq job for the first root-level subjob' do
        jobs = @queue.to_a
        first_job = jobs.first

        jobs.count.should eq(1)
        first_job.klass.should == 'Worker1'
        first_job.args.should == [100]
      end
    end
  end

  describe '.perform_async cascade' do
    after :each do
      clean_datastores
    end

    context 'batch superworker' do
      before :each do
        BatchSuperworker.perform_async(user_ids: [100, 101])
      end

      # subjob_id - subworker_class
      # 1 - batch
      # 2 - batch_child
      # 3 - Worker1
      # 4 - Worker2
      # 5 - batch_child
      # 6 - Worker1
      # 7 - Worker2

      it 'sets the correct initial statuses' do
        subjob_statuses_should_equal(
          [1, 2, 5] => 'running',
          [3, 6] => 'queued',
          [4, 7] => 'initialized'
        )
      end

      it 'sets the correct statuses after subjob #3 completes' do
        trigger_completion_of_sidekiq_job(3)
        subjob_statuses_should_equal(
          [1, 2, 5] => 'running',
          [3] => 'complete',
          [7] => 'initialized',
          [4, 6] => 'queued'
        )
      end

      it 'sets the correct statuses after subjob #4 completes' do
        trigger_completion_of_sidekiq_job(3)
        trigger_completion_of_sidekiq_job(4)
        subjob_statuses_should_equal(
          [1, 5] => 'running',
          [2, 3, 4] => 'complete',
          [7] => 'initialized',
          [6] => 'queued'
        )
      end
    end

    context 'complex superworker' do
      before :each do
        worker_perform_async(ComplexSuperworker)
      end

      it 'sets the correct statuses after subjob #1 completes' do
        trigger_completion_of_sidekiq_job(1)
        subjob_statuses_should_equal(
          1 => 'complete',
          2 => 'queued',
          (3..5) => 'initialized'
        )
      end

      it 'sets the correct statuses after subjob #2 completes' do
        trigger_completion_of_sidekiq_job(1)
        trigger_completion_of_sidekiq_job(2)
        subjob_statuses_should_equal(
          1 => 'complete',
          2 => 'complete',
          3 => 'queued',
          (4..5) => 'initialized'
        )
      end

      it 'sets the correct statuses after subjob #3 completes' do
        trigger_completion_of_sidekiq_job(1)
        trigger_completion_of_sidekiq_job(2)
        trigger_completion_of_sidekiq_job(3)
        subjob_statuses_should_equal(
          1 => 'complete',
          2 => 'complete',
          3 => 'complete',
          4 => 'queued',
          5 => 'initialized'
        )
      end

      it 'sets the correct statuses after subjob #4 completes' do
        trigger_completion_of_sidekiq_job(1)
        trigger_completion_of_sidekiq_job(2)
        trigger_completion_of_sidekiq_job(3)
        trigger_completion_of_sidekiq_job(4)
        subjob_statuses_should_equal(
          1 => 'complete',
          2 => 'complete',
          3 => 'complete',
          4 => 'complete',
          5 => 'queued'
        )
      end

      it 'sets the correct statuses after subjob #6 completes' do
        trigger_completion_of_sidekiq_job(1)
        trigger_completion_of_sidekiq_job(2)
        trigger_completion_of_sidekiq_job(3)
        trigger_completion_of_sidekiq_job(4)
        trigger_completion_of_sidekiq_job(5)
        subjob_statuses_should_equal(
          1 => 'complete',
          2 => 'complete',
          3 => 'complete',
          4 => 'complete',
          5 => 'complete'
        )
      end
    end

    describe 'job completion' do
      it 'removes all jobs after superworker completion if option is set accordingly' do
        original_value = Sidekiq::Superworker.options[:delete_subjobs_after_superjob_completes]
        Sidekiq::Superworker.options[:delete_subjobs_after_superjob_completes] = true

        worker_perform_async(ComplexSuperworker)
        trigger_completion_of_sidekiq_job(1)
        trigger_completion_of_sidekiq_job(2)
        trigger_completion_of_sidekiq_job(3)
        trigger_completion_of_sidekiq_job(4)
        trigger_completion_of_sidekiq_job(5)

        Sidekiq::Superworker.options[:delete_subjobs_after_superjob_completes] = original_value
        Sidekiq::Superworker::Subjob.count.should == 0
      end
    end

    describe 'failing workers' do
      it "sets the status to 'failed'" do
        superjob_id = FailingSuperworker.perform_async(first_argument: 100)
        trigger_completion_of_sidekiq_job(1)
        subjob_statuses_should_equal(
          1 => 'complete',
          2 => 'queued'
        )
      end

      it "doesn't run jobs downstream from the failure" do
        superjob_id = FailingSuperworker.perform_async(first_argument: 100)
        trigger_exception_in_sidekiq_job(1)
        subjob_statuses_should_equal(
          1 => 'failed',
          2 => 'initialized'
        )
      end
    end
  end

  def worker_perform_async(worker)
    worker.perform_async(first_argument: 100, second_argument: 101)
  end
end
