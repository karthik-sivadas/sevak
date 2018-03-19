module Sevak

  # Base class for all queue consumers, all consumers should inherit from the base Sevak::Consumer and must implement a
  # run method. The run method should implement the business logic.

  class ConsumerBase

    include Core
    include Autoscale

    DEFAULT_PREFETCH_COUNT = 10

    # class methods
      def self.queue_name(name='default')
        @queue_name ||= name
      end

      def self.autoscale(value=false)
        @autoscale ||= value
      end
    # end of class methods

    def initialize
      @queue_name = self.class.queue_name
      @autoscale = self.class.autoscale
    end

    def queue_name
      @queue_name
    end

    def autoscale
      @autoscale
    end

    def autoscale=(value)
      @autoscale = value
    end

    def queue
      @queue ||= channel.queue(queue_name, durable: true)
    end

    def channel
      @channel ||= connection.create_channel
    end

    def message_count
      queue.message_count
    end

    def initiate_consumer
      puts 'Queue Name: ' + queue_name
      puts connection.closed?
      channel.prefetch(config.prefetch_count || DEFAULT_PREFETCH_COUNT)

      queue.subscribe(manual_ack: true, exclusive: false) do |delivery_info, metadata, payload|
        body = JSON.parse(payload)

        # p delivery_info
        # p metadata

        begin
          status = run(body)
        rescue => ex
          Sevak.log(exception_details(ex, payload))
          status = :error
        end

        if status == :ok
          channel.ack(delivery_info.delivery_tag)
        elsif status == :retry
          channel.reject(delivery_info.delivery_tag, true)
        else # :error, nil etc
          channel.reject(delivery_info.delivery_tag, false)
        end
      end

      wait_for_threads
    end

    def start
      @autoscale ? decision_maker : initiate_consumer
    end

    def wait_for_threads
      sleep
    end

    def exception_details(e, payload = nil)
      h = {
        source: "#{self.class}",
        type: "#{e.class}",
        message: e.message,
        payload: payload.inspect,
        backtrace: (e.backtrace || []).take(3).join("\n")
      }

      msg = h.map { |k,v| "#{k.to_s.capitalize}: #{v.to_s}"}.join(' | ')

      "Sevak Exception: #{msg}"
    end

  end

  class Consumer < ConsumerBase

    # Set the queue name for the consumer
    queue_name 'sevak.default'
    autoscale false
    

    def run(payload)
      puts 'class run'
      # implement business logic in the corresponding consumer, the run method should respond with
      # status :ok, :error, :retry after the processing is over
      Sevak.log("Implement run method. Payload: #{payload.inspect} #{}")
      :ok
    end
  end
end