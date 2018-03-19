
module Sevak
	
	module Autoscale
		MAX_CONSUMERS_LIMIT = 5
		MIN_CONSUMER_LIMIT  =  1
		MAX_MESSAGE_PER_CONSUMER = 10
	
		ALL_CONSUMERS = []

		def spawn_consumer
			pid = fork do
				
				c = self.class.new()
				c.autoscale = false
				Signal.trap("USR1") do
					c.cancel_consumer
				end
				c.start
			end
			ALL_CONSUMERS << pid
		end

		def kill_consumer
			pid = ALL_CONSUMERS.shift
			puts 'killing process: ' + pid.to_s 
			Process.kill('USR1', pid)
			Process.wait2
		end

		def take_scaling_decision
			curr_size = ALL_CONSUMERS.size
			curr_message_count = message_count
			consumers_needed = (curr_message_count.fdiv( MAX_MESSAGE_PER_CONSUMER )).ceil
			if curr_size > consumers_needed
				'down'
			elsif (curr_size < consumers_needed) && (ALL_CONSUMERS.size < MAX_CONSUMERS_LIMIT)
				'up'
			else
				'do_nothing'
			end
		end
		
		def decision_maker
			puts 'decision maker'
			loop do
				decision = take_scaling_decision
				if decision == 'up'
					spawn_consumer
				elsif decision == 'down'
					kill_consumer
				else 
					'do_nothing'
				end
				sleep 60
			end
		end
			
	end
	
end
	