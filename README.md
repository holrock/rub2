* There's the rub

wrapper for torque qsub

* sample

```ruby
require 'rub2'

submit "SimpleJob" do
  execute_with Dir.glob("/etc/*.conf") do |file|
     "wc -l #{file}"
  end
end
# exit if job failed


submit "WithOptions" do
  log 'log/test.log'                                              # log file path
  resource 'nodes' => '1:ppn=4', 'mem' => '15mb'                  # qsub -l option
  array_request 2..4 # or array_request [1, 3]                    # qsub -t option
  inherit_environment                                             # qsub -V option
  continue_on_error                                               # don't exit on job failed
  dry_run                                                         # output script and exit. no execute

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
```

* 使い方

submit "JobName"で指定された名前でジョブを作成します。
submitブロック内でexecute_withの引数に配列を渡すと、配列の要素数分JobArrayを作成して、execute_withから返された文字列をbashの引数として実行します。
jobが全て成功するとsubmitブロックはtrueを返します。

* オプション

** log

ログファイル出力先。未指定の場合はカレントフォルダにログディレクトリを作成します。

** resource

qsub -lオプション。リソース名 => 値のハッシュを渡してください。

** array_request

実行するarray index。1-10の時は1..10のようにRangeを、1,2,4の時は[1,2,4]と配列を渡してください。

** inherit_environment

qsub -Vオプション。環境変数を引き継ぎます。

** dry_run

実行せずにsubmitするスクリプトを表示します。デバッグ向け。

** max_retry

max retry count

* ハンドラ

ジョブが成功した場合にはon_doneハンドラ、一つでも失敗した場合はon_failハンドラで指定されたブロックを実行します。
未指定の場合はデフォルトハンドラを実行し、失敗したジョブがあれば表示します。


その他

各jobの結果はRinda(dRuby)サーバーで受け取ります。 ネットワーク不調などにより結果が受け取れず終了しないときは適当にctrl-cで終らせてくさい。
