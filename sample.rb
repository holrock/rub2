#!/usr/bin/env ruby

require 'rub2'

submit "SimpleJob" do
  execute_with Dir.glob("/etc/*.conf") do |dotfile|
     "wc -l #{dotfile}"
  end
end


submit "WithOptions" do
  log 'log/test.log'                                              # log file path
  resource 'nodes' => '1:ppn=4', 'mem' => '15mb'                  # qsub -l option
  array_request 2..4 # or array_request [1, 3]                    # qsub -t option
  inherit_environment                                             # qsub -V option
  continue_on_error                                               # don't exit on job error
  dry_run                                                         # output script and exit. no execute
  max_retry 5

  # multiple arguments
  execute_with [1, 2, 3, 4], [4, 5, 6, 7] do |arg1, arg2|
     "echo '#{arg1} + #{arg2}' | bc"
  end

  # succeeded handler
  on_done do
    puts 'done'
  end

  # faild handler
  # results: {job_id => ret_code}
  on_fail do |results|
    results.each do |job_id, ret_code|
      puts get_executed_command(job_id) unless ret_code == 0
    end
  end
end
