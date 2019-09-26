require "rub2/version"

require 'erb'
require 'pathname'
require 'open3'
require 'rinda/tuplespace'

module Rub2

  def putlog(str)
    s = Time.now.strftime("%FT%T")
    print "#{s}\t#{str}\n"
  end
  module_function :putlog

  class JobScript
    attr_accessor :log,
                  :shell,
                  :resource,
                  :array_request,
                  :inherit_environment,
                  :commands,
                  :log_path,
                  :uri,
                  :queue
    attr_reader :source, :name

    def initialize(name)
      @name = name
      @shell = '/bin/bash'
      @log_path = make_default_log_path(name)
      @uri = nil
    end

    def build
      @source = ERB.new(ScriptTemplate, nil, '-').result(binding)
    end

    # Range (1..100) or Array [1,10,50] -> '-t 1-100' or -t '1,10,50'
    def make_array_request_string
      @array_request = (1..@commands.size) if @array_request.nil?
      limit = ''
      unless @slot_limit.nil?
        limit = "%#{@slot_limit}"
      end

      if @array_request.kind_of?(Range)
        last = nil
        if @array_request.exclude_end?
          last = @array_request.last - 1
        else
          last = @array_request.last
        end
        return sprintf("%d-%d%s", @array_request.first, last, limit)
      end
      return @array_request.sort.join(',') + limit
    end

    private

    # return path to "pwd/log_ymd_hms/jobname_log"
    def make_default_log_path(name)
      t = Time.now.strftime('%Y%m%d_%H%M%S')
      Pathname.new(Dir.pwd).join("log_#{t}", "#{name}.log")
    end

    # return key=value[,key=value]
    def make_pbs_resources_string
      return '' if @resource.empty?

      s = []
      @resource.each do |k, v|
        s.push("#{k}=#{v}")
      end
      return s.join(',')
    end

    ScriptTemplate =<<'EOS'
#PBS -S <%= @shell %>
#PBS -N <%= @name %>
#PBS -j oe
#PBS -o <%= @log_path %>
#PBS -t <%= make_array_request_string %>
<%- unless @resource.nil? -%>
#PBS -l <%= make_pbs_resources_string %>
<%- end -%>
<%- if @inherit_environment -%>
#PBS -V
<%- end -%>
<%- if @queue -%>
#PBS -q <%= @queue %>
<%- end -%>

CMD=(
<% @commands.each do |i| -%>
"<%= i.gsub(/"/){ '\\"' } %>"
<% end -%>
)
ID=$(($PBS_ARRAYID - 1))
cd $PBS_O_WORKDIR
echo "job start: $(date -Iminute)"
echo "$PBS_O_HOST -> $(hostname): $PBS_JOBNAME $PBS_JOBID (cwd: $PWD)"
echo "execute: ${CMD[$ID]}"
START_TIME=$(date +%s)
bash -c "set -e; set -o pipefail; ${CMD[$ID]}"
RET=$?
echo "job exit: $RET at: $(date -Iminute)"
EXIT_TIME=$(date +%s)
for i in {0..10}; do
  ruby -r drb -e "DRbObject.new_with_uri('<%= @uri %>').write([<%= Process.pid %>, '$PBS_JOBID', $PBS_ARRAYID, '$HOSTNAME', $RET, $START_TIME, $EXIT_TIME])"
  if [ $? -eq 0 ]; then
    exit $RET
  fi
  sleep 5
done
echo GIVEUP
exit $RET
EOS

    end

    class Job
      attr_reader :parent_id, :array_id, :exit_code, :died_at

      def initialize(parent_id, array_id, max_retry)
        @parent_id = parent_id
        @array_id = array_id
        @max_retry = max_retry
        raise "negative max_retry" if max_retry < 0
        @exit_code = nil
        @retry_count = 0
        @died_at = nil
      end

      def finished?
        return false if @exit_code.nil?
        return (not need_retry?)
      end

      def succeeded?
        return @exit_code == 0
      end

      def need_retry?
        return false if @exit_code.nil?
        return false if succeeded?
        return @retry_count < @max_retry
      end

      def job_id
        return "#{@parent_id}[#{@array_id}]"
      end

      def set_exit_code(exit_code)
        unless @exit_code.nil?
          Rub2.putlog("warn: already assigned exit_code #{self} #{@exit_code} -> #{exit_code}")
        end
        @exit_code = exit_code
        @died_at = nil
      end

      def dead_end?(t)
        return false if @died_at.nil?
        return (t - @died_at) > 60.0 # daed job, maybe
      end

      def set_dead_time(t)
        @died_at = t
      end

      def set_resubmit_id(new_parent_id)
        @parent_id = new_parent_id
        @exit_code = nil
        @died_at = nil
        @retry_count += 1
      end

      def inspect
        "#<Job: @parent_id=#{@parent_id}, @array_id=#{@array_id}, @exit_code=#{@exit_code}, @retry_count=#{@retry_count}, @max_retry=#{@max_retry}, @died_at=#{@died_at}>"
      end

      def to_s
        return job_id
      end
  end


  class JobStore

    def initialize
      @jobs = nil
    end

    def init_job(job_id, job_index, max_retry)
      raise "already initialized" unless @jobs.nil?
      jobs = []
      job_index.each do |index|
        jobs.push(Job.new(job_id, index, max_retry))
      end
      @jobs = jobs
    end

    def get_job_from_index(index)
      return @jobs.find {|j| j.array_id == index}
    end

    # exit_status: [ {:array_id => 1, :exit_code => 1}, ...]
    def update_exit_code(exit_status)
      exit_status.each do |es|
        job = get_job_from_index(es[:array_id])
        job.set_exit_code(es[:exit_code])
      end
    end

    def mark_dead_job(jobs)
      now = Time.now
      jobs.each do |job|
        if job.dead_end?(now)
          job.set_exit_code(-1)
          Rub2.putlog("#{job} no response 1min after finished.")
        else
          job.set_dead_time(now)
        end
      end
    end

    def each_job(&block)
      @jobs.each do |job|
        yield job
      end
    end

    def all_finish?
      return @jobs.all? {|j| j.finished?}
    end

    def select_retry_jobs
      return @jobs.select {|job| job.need_retry?}
    end

    def job_count
      return @jobs.size
    end
  end

  class DeadJobCollector

    def collect(job_store)
      runnning_job_ids = parse_qstat(`qstat -t 2>/dev/null`)
      failed = []
      job_store.each_job do |job|
        failed.push(job) if dead_job?(job, runnning_job_ids)
      end
      job_store.mark_dead_job(failed)
    end

    private

    def parse_qstat(str)
      id_hash = {}
      str.each_line do |line|
        if line =~ /\A(\d+(:?\[\d+\])?)/
          id_hash[$1] = 1
        end
      end
      return id_hash
    end

    def dead_job?(job, runnning_job_ids)
      return false if runnning_job_ids.has_key?(job.job_id)
      return (not job.finished?)
    end

  end

  class JobResultCollector
    def initialize(uri, timeout, job_count)
      @drb = DRbObject.new_with_uri(uri)
      @pid = Process.pid
      @timeout = timeout
      @job_count = job_count
      @success_count = 0
    end

    # block thread
    def collect_job_result(job_store)
      _pid, job_id, array_id, host, exit_code, start_time, exit_time = @drb.take([@pid, nil, nil, nil, nil, nil, nil], @timeout)
      job_store.update_exit_code([{:array_id => array_id, :exit_code => exit_code}])
      @success_count += 1 if exit_code == 0
      t = Time.at(exit_time) - Time.at(start_time)
      min, sec = t.divmod(60)
      Rub2.putlog "#{job_id}(#{host}) => #{exit_code}\t[#{min}m#{sec.truncate}s]\t(#{@success_count}/#{@job_count})"
    end
  end

  class Manager
    def initialize(name)
      @script = JobScript.new(name)
      @job_store = JobStore.new
      @timeout = 30
      @max_retry_count = 0
      @jobid = []
    end

    # example:
    # execute_with array (, arrays) do |arg1 (, args...)|
    #   return command_string
    # end
    def execute_with(first, *rest, &block)
      commands = []
      first.zip(*rest) do |i|
        cmd = block.call(*i)
        commands.push cmd if cmd
      end
      @script.commands = commands
    end

    ### handler

    # example: on_fail {|results| p results}
    def on_fail(&block)
      @fail_proc = block
    end

    # example: on_done {puts 'done'}
    def on_done(&block)
      @done_proc = block
    end

    ### job options

    def log(log_path)
      @script.log_path = Pathname.new(log_path)
    end

    def shell(intep)
      @script.shell = intep
    end

    def resource(res = {})
      @script.resource = res
    end

    def array_request(req)
      @script.array_request = req
    end

    # slot limit doent't work on torque
    def slot_limit(limit)
      @script.slot_limit = limit
    end

    def dry_run
      @dry_run = true
    end

    def inherit_environment
      @script.inherit_environment = true
    end

    def queue(q)
      @script.queue = q
    end

    def continue_on_error
      @continue_on_error = true
    end

    def max_retry(count)
      @max_retry_count = count
    end

    ### accessor

    def get_executed_command(job_id)
      return @script.commands[job_id - 1]
    end

    ### job control

    # exec qsub
    def submit
      unless @dry_run
        @script.log_path.dirname.mkpath unless @script.log_path.dirname.exist?
        @script.uri = start_tuplespace
      end

      @script.build


      if @dry_run
        puts @script.source
        return
      end
      @jobid << submit_qsub(@script.source)
      @job_store.init_job(@jobid.first, @script.array_request, @max_retry_count)
    end

    def wait_finish
      return true if @dry_run

      results = polling_loop

      if results.all? {|aid, ret| ret == 0}
        if @done_proc
          @done_proc.call
        else
          Rub2.putlog "job succeeded"
        end
        return true
      end

      if @fail_proc
        @fail_proc.call(results)
      else
        results.each do |aid, ret|
          unless ret == 0
            Rub2.putlog "array job failed: #{ret}"
          end
        end
      end

      return false if @continue_on_error
      Rub2.putlog "job failed: #{@jobid.join(',')}"
      exit false
    end

    private

    def polling_loop
      job_result = JobResultCollector.new(@uri, @timeout, @job_store.job_count)
      dead_job = DeadJobCollector.new()

      until @job_store.all_finish?
        begin
          job_result.collect_job_result(@job_store) until @job_store.all_finish?
        rescue Rinda::RequestExpiredError
          # ignore timeout
        end
        dead_job.collect(@job_store)
        retry_job = @job_store.select_retry_jobs
        resubmit_faild_job(retry_job) unless retry_job.empty?
      end
      results = []
      @job_store.each_job do |job|
        results.push([job.array_id, job.exit_code])
      end
      return results
    end

    def resubmit_faild_job(failed_jobs)
      return if failed_jobs.empty?
      ids = failed_jobs.map {|job| job.array_id}.join(',')
      newjobid = submit_qsub(@script.source, ids)
      @jobid << newjobid
      failed_jobs.each {|job| job.set_resubmit_id(newjobid)}
    end

    def submit_qsub(script, array_option = nil)
      jobid = nil
      cmd = "qsub"
      cmd += " -t #{array_option}" if array_option
      Open3.popen3(cmd) do |stdin, stdout, stderr|
        stdin.puts(script)
        stdin.close
        jobid = stdout.read.chomp
        raise "qsub error: " + stderr.read.chomp if jobid.empty?
      end
      jobid =~ /\A(\d+)/
      jobid = $1
      Rub2.putlog "job submited: #{jobid}[#{array_option || @script.make_array_request_string}]"
      return jobid
    end

    # start server for job results
    def start_tuplespace
      @ts = Rinda::TupleSpace.new
      @drb = DRb.start_service("druby://:0", @ts)
      @uri = @drb.uri
      Rub2.putlog "start Rinda Server: #{@uri}"
      return @uri
    end

  end
end

# define global DSL function
def submit(name, &block)
  raise "Empty name" if name.empty?
  job = Rub2::Manager.new(name)
  job.instance_eval(&block)
  job.submit
  return job.wait_finish
end
