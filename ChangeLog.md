# Change Log

## 1.3.0 (Jan 21 2016)

* change unit test to pass in the poor env [hiroaki-iwase] f0d9cc8

## 1.3.0RC1 (Nov 13 2015)

* Update README.md [Paras Patel] 3350844
* Automation some manual tests [hiroaki-iwase] dea72fa
  * auto_recover
  * set_routing_trans_timeout
  * st_class
  * adm_tool
  * check_enable_repeathost
  * check_replication_in_host
* add below func's unit test [hiroaki-iwase] f9a1721
 * jaro-winkler 
 * stat log level 
 * stat secondary 
 * shutdown_self 
 * stat failover 
 * get_key_info 
 * enabled_repetition_in_routing? 
 * switch_dns_caching 
 * add_rttable_sub_nid 
 * delete_rttable_sub_nid 
 * clear_rttable_sub_nid 
 * del_latency_avg_calc_cmd 
 * add_latency_avg_calc_cmd 
 * chg_latency_avg_calc_time_count 
 * set_latency_avg_calc_rule
* add test of below func [hiroaki-iwase] 613c0d0
  * stat log level 
  * stat secondary 
  * shutdown_self
* add unit test of jaro winkler [hiroaki-iwase] 22fd7b2
* remove puts method in testfile [hiroaki-iwase] cec8a6d
* implement cluster replication function [hiroaki-iwase] a1fa5e8
* implement get_expt command [hiroaki-iwase] 04f1a14
* change log level of push_a_vnode_stream from info to debug [hiroaki-iwase] e668fc0
* add how to contributing [hiroaki-iwase] 6d9945a
* add progress rate to data accumulation tool [hiroaki-iwase] 2f9d8aa
* implement consistency check tool [Hiroaki Iwase] 3211bd9

## 1.2.0 (Aug 19 2015)

* Change gemspec [Hiroaki Iwase] db2bb42
* Modify unit-test [Hiroaki Iwase] 7479034
* Adjust secondary node to redundancy over3 [Hiroaki Iwase] a6fafd8
* Add new func to check tokyo cabinet additional flag [Hiroaki Iwase] e17b3af
* Make new tool(roma-adm) [Hiroaki Iwase] 8cf2a03
* Add new func to check config.rb version when booting [Hiroaki Iwase] 1befb8f
* Add new func to check command miss spell by jaro-winkler [Hiroaki Iwase] 607c96e
* Enable to check failover status on stats[Hiroaki Iwase] d5a6a9f
* Enable to check log level on stats [Hiroaki Iwase] 14b1455
* Modify duplicate problem(plugin_storage.rb) of mkconfig script [Hiroaki Iwase] c057191
* Adjust groonga [Hiroaki Iwase] 0ee097c
* Add new command(shutdown_self) [Hiroaki Iwase] 8bb0aa5
* add new testcase about cpdb as Groonga msg [ooeyoshinori] 6744580
* Fix random routing logic for join [Hiroki Matsue] ea29af3
* Support new stats interface of client and refactor codes. [Hiroki Matsue] fd65292
* Add tests for routing logic and refactor tests [Hiroki Matsue] 1d52cbe
* Add Dynamic Log Shift size and age Test  Script [Paras Patel] 081ebd2
* Remove unnecessary path in ssroute booting file [Hiroaki Iwase] 4f678cd

## 1.1.0 (Mar 30 2015)

* I improved connection performance during booting [Takahiro Tokunaga] ee411f0
* change support ruby version [hiroaki-iwase] 69c9cab
* Update gemspec. [Hiroki Matsue] b2301de
* remove booting error in the log [Takahiro Tokunaga] c0cfd68
* Modify routing files checking condition. [Hiroki Matsue] c7bd4ab
* Remove 'Support' area and modify some commands. [Satoko Kurobe] 0146afe
* add new commands (set_log_shift_size, set_log_shift_age) [Masami Tazuke] 5d03bee
* Fix redirect loop of GUI. [Hiroki Matsue] 53a2841
* Improve Log Gathering logic [Hiroaki Iwase] dfcaa8e
  * add hilatecy check [hiroaki-iwase] 78b0d24
  * add LOG_STREAM_SHOW_WAIT_PARAM when show_logs cmd display logs [hiroaki-iwase] c8f237b
  * change to recursive algorithm [hiroaki-iwase] 6009e53
  * add gathered date for expiration in clean_up process [hiroaki-iwase] d26bb58
  * support outrange direction [hiroaki-iwase] 420c1f8
  * enable to point out start-date and end-date of gathering target [hiroaki-iwase] e1c60dd
* Correct some URLs. [Satoko Kurobe] d941dc9
* Remove README and add README.md [Satoko Kurobe] a9a9afc
* allow node name to only ascii without empty string. [hiroaki-iwase] f05600d
* add check argument in setroute [hiroaki-iwase] 6b60675
* remove github issue url [Takahiro Tokunaga] 94feeed
* add replication_in_host option [Takahiro Tokunaga] 2284a53
* Define rome.gemspec [Tatsuya Sato] e4c4a89

## 1.1.0RC2 (Jan 19 2015)

* modify merge prob(delete ruby/server dir, mv groonga relating file) [hiroaki-iwase] 699e0b7

## 1.1.0RC1 (Jan 7 2015)

* allow cpdb in just case of RubyHashStorage & TCStorage [hiroaki-iwase] 42ef43b
* add storage type checking to cpdb [hiroaki-iwase] 47c4e56
* delete server directory [Takahiro Tokunaga] 75ee999
* Abolish ruby server dir [satoryu] bfe2339
* Declare the license of ROMA in gemspec file [satoryu] c644f93
* Add Groonga based storage backend [Kouhei Sutou] fd993d3
* Use auto test runner [Kouhei Sutou] 2e8367f
* Use test-unit gem [Kouhei Sutou] 81677d1

## 1.0.0 (Sep 30 2014)

* Add new function to get vnodes information of each data [hiroaki-iwase] ad25c6e
* Add new function to get key list without stopping ROMA [hiroaki-iwase] f36e907
* Add new GUI control system(Gladiator) [hiroaki-iwase] 66dfd58
* Add new function to get log data via ROMA console [hiroaki-iwase] b1995e4
* change some snapshot and storage logic [hiroaki-iwase] 257b0df
* Add new function to check routing consistecy [hiroaki-iwase] dcd1ace
* Add new function of getting routing event [hiroaki-iwase] e297215
* fix : no data copy in the join process when 2 hosts, 2 processes [junji torii] 10eae74
* Merge branch 'master' of git@github.com:roma/roma into fix_join [junji torii] 41e0269
* refacort:error case for the release command. [junji torii] 0144b63
* change release logic [hiroaki-iwase] f950fa1
* bugfix:end condition of recover process in no_action [junji torii] 9b41a3c
* {func}__prev support for DSL [junji torii] d772c56
* Add new function to get first instance information [hiroaki-iwase] e4b465f
* {func}__prev support in write-behind [junji torii] 96af508
* each_vn_dump supported in normal status [junji torii] 546ee7d

## 0.8.14 (Mar 26 2014)

* add timeout parameter for a vnode copy [junji torii] 0522750
* add command to change routing_trans_timeout [Rui Bando] 8247698
* Added command to change klength and vlength limit when vnode copy [Paras Patel] fd14206
* Add spushv_read_timeout and reqpushv_timeout_count set Method [Paras Patel] 264d3ec
* fix : add rescue in timer_event_1sec [junji torii] 11cc4ea
* fix : warning of class variable access from toplevel(for adjust Ruby2.1.1) [junji torii] 9df1a5c
* fix : modify default value(nil -> false) of autorecover func [hiroaki-iwase] 30b18c5
* add snapshot function [junji torii] 2ab928f
* add safecopy_integration_test for snapshot [hiroaki-iwase] 8e799c1

## 0.8.13-p1 (Jan 15 2014)

* add timeout parameter for a vnode copy [junji torii] 0522750
* remove some debug log [hiroaki-iwase] 94d96dc
* Add Gemfile. [Hiroki Matsue] 9029bf8
* Ignore test related files. [Hiroki Matsue] 556a931
* fix:add rescue in timer_event_1sec [junji torii] 11cc4ea

## 0.8.13 (Sep 12 2013)

* change target ruby version about encoding problem (upper 1.9.2 => upper 1.9.1) [hiroaki-iwase] e56d2f4
* modify end message of join. [hiroaki-iwase] a649dfb
* modify typo [hiroaki-iwase] c47802b
* fix:for a problem of encording on 1.9.2 [junji torii] 3501622
* modify ongoing setting changing cmd. [hiroaki-iwase] 190db49
* Add new async process for calculate latency average & add some commands for change setting. [hiroaki-iwase] 49dc9cb
* add new function "calculate latency average" [hiroaki-iwase] 40b6266
* Add command of changing CONNECTION_POOL_EXPTIME & DEFAULT_LOST_ACTION & CONNECTION_DESCRIPTOR_TABLE_SIZE & WRITEBEHIND_SHIFT_SIZE [hiroaki-iwase] de757af
* add vnode balancing logic [junji torii] bfa16cd
* add *_rttable_sub_nid commands [junji torii] 8bd4a31
* add auto_recover function [hiroaki-iwase] 473df72
* modify message when execute "balse" [hiroaki-iwase] a3d61b1
* add node-id substitution feature in routingdump command. [junji torii] a3693e8
* fixed recoverlost_lib's value error [hiroaki-iwase] deb9430
* add log output(info level) when new wb file was created. [hiroaki-iwase] dcec49a
* add new function "calculate latency average" [hiroaki-iwase] 4158ccd
* Add DNS caching function. [Hiroki Matsue] abf4464
* Modify some default values. Change plugin selecting(plugin_storage.rb is set without question). [hiroaki-iwase] d2542f0
* add command of changing CONNECTION_EMPOOL_EXPTIME. [hiroaki-iwase] 1d3df93
* improve mkconfig.rb [hiroaki-iwase] 74be88d
* remove utf-8 manifesto # -*- coding: utf-8 -*-" # encoding: utf-8 [hiroaki-iwase] d54e30c
* translate comment JP to ENG [hiroaki-iwase] 1b091fd

## 0.8.12 (Feb 12 2013)

* refactor: for protocol [Hiroki Matsue] a6f9152
* Add test for context switch with eventmachine. [Hiroki Matsue] 50d0a84
* bugfix : Fixed when null is included in a list. [junji torii] 5436d3a
* Delete duplicated tests. [Hiroki Matsue] 3ba6a12
* refactor:for balance command [junji torii] 2216d61
* Modify each_vn_dump method. [Hiroki Matsue] 267fed9
* Delete sync command. [junji torii] 51daf17
* Delete dumpfile command. [junji torii] a6d8697
* refactor:Delete old command. [junji torii] 5595060
* refactor: for spushv_protection flag [junji torii] d6eaece
* Add spushv_protection flag [junji torii] e9efa79
* refactor:for release process [junji torii] 076228b
* refactor:join process [junji torii] f920dbc
* Add recover function in the partitioner module. [junji torii] 6a79cff
* refactor:Add alias. [junji torii] 7f31cb3
* fix:Protects the 'reqpushv' request in the join process. [junji torii] 0f7f106
* Delete a deprecated option and method in recover command. [junji torii] 8490094
* fix:run_iterate_storage flag is used in one thread. [junji torii] c273bcc
* refactor:Add new functions. [junji torii] debb5f6
* refactor:Delete a unused method. [junji torii] 5aae744
* refactor:Use the __method__ in log messages. [junji torii] a36fa0c
* refactor:Starting and stopping timing of cleanup process. [junji torii] 307bf73
* refactor:Unused code deleted. [junji torii] 7e4bbe7
* fix:Starting and stopping timing of cleanup process [junji torii] 0e1fc0d
* add random partitioner [junji torii] 190f58e
* add join_process [junji torii] 7879674
* Modify set command safer. [Hiroki Matsue] 91de5a8
* fix:for a problem of encording on 1.9.3 [junji torii] 0c68a58

## 0.8.11 (Aug 8 2012)

* Allocate cmd_aliases plugin. [firejun] f7555c2
* Add a tool for log. [firejun] bea6c87
* Fix timezone of mapcount plugin and add tests. [Hiroki Matsue] 11f36b8
* Make TCMemStorage available to set options. [Hiroki Matsue] 0fef58b
* Correct end status of release command. [Hiroki Matsue] fc9a4bc
* Fix release command feature in a small nodes. [Hiroki Matsue] c765358
* fix:[can't add a new key into hash during iteration] occurring in leter then ruby 1.9.1 fixed. [junji torii] 2987536
* Fix result view of mkconfig. [Hiroki Matsue] bfb0f4a

## 0.8.10 (Feb 20 2012)

* Add authors and homepage info to gem. [Hiroki Matsue] 35c5db4
* Change default number of CONNECTION_EXPTIME. [Hiroki Matsue] 48933ca
* Allocate mapcount plugin. [Hiroki Matsue] 5cc9d6b
* Fix calculate part. [Hiroki Matsue] a9e0642
* Fit DATACOPY_STREAM_COPY_WAIT_PARAM to dcnice. [Hiroki Matsue] ac6a2df
* Adjust Rakefile to multi ruby version. [Hiroki Matsue] d5ae69e
* Adjust Rakefile to new directory composition. [Hiroki Matsue] 8897920
* Change simbol key to string to commonalize. [Hiroki Matsue] 0f42743
* Add methods *_ms to return serialized value by Marshal. [Hiroki Matsue] 9596207
* Change serialize format json to marshal. [Hiroki Matsue] 1cd1f9b
* Remove commons directory. [Hiroki Matsue] fd235e5
* Relocate files of commons directory to server directory. [Hiroki Matsue] 3e3f017
* Fix filelist to make gem. [Hiroki Matsue] 28e901f
* Fix path handling way. [Hiroki Matsue] 9ce3f5a
* Remove double-byte space and change encoding. [Hiroki Matsue] adeed9c
* Refactor mkconfig tool. [Hiroki Matsue] f88f78f
* Add a tool for setting cofigurations. [Hiroki Matsue] adb1a6c
* Change data unpack method name. [Hiroki Matsue] c810b4c
* Change Symbol keys to String keys for MessagePack. [Hiroki Matsue] 854566c
* Change serialize format Marshal to Message pack. [Hiroki Matsue] 297260a
* Fix mapcount_update method to handle empty key. [Hiroki Matsue] a50cb28
* Activate subkeys at update method. [Hiroki Matsue] 36c6372
* Add test for mapcount_get method. [Hiroki Matsue] bb6ea7c
* implement mapcount_get to support sub_keys. [Hiroki Matsue] 4c9be9a
* fix module name [yukio.goto] 3c0e21c

## 0.8.9 (Dec 16 2011)

* add chg_time_expt() method [junji torii] 305cefc
* implement to check hash does not exist. [yukio.goto] cef82e0

## 0.8.8 (Jun 30 2011)

* create a lock file in the open method,and remove a lock file in the close method. [junji torii] 874f76a
* for dynamic write-behind [junji torii] 0885028

## 0.8.7 (Nov 24 2010)

* refactor: add storage exception [junji torii] 5739597

## 0.8.6 (Oct 20 2010)

* refactor: class macro for a plugin [junji torii] 7d89e4e
* add a map plugin [junji torii] 7db2f64
* bugfix: restart eventmachine by unbind event [junji torii] c457a70

## 0.8.5 (Aug 16 2010)

* bugfix: for zero byte read [junji torii] ef2dada
* bugfix: for connection expire logic [junji torii] 0d883f6

## 0.8.4 (Jun 21 2010)

* refactor: modified a result of a stats command [junji torii] 9e966ed
* bugfix: connection leak was corrected when a connection expired in irregular connection [junji torii] d23bde9
* bugfix: access to a nil object in a logging was corrected [junji torii] dae2ed8
* added a selection of a system call configuration, which of an epoll or select [junji torii] 24c41e2
* refactor: supported epoll [junji torii] 5c777f1

## 0.8.3 (Jun 11 2010)

* added a new command for change to max length of a connection pool [junji torii] 1fbbe29
* added a switch_failover command [junji torii] 6489383
* added a configuration of connection management items [junji torii] 1f3fbd7
* added a feature of expired connection [junji torii] b99e2e7
* added a new option to the romad for unit-test, that command protection disable while starting [junji torii] e2a6af4
* refactor:support for a REJECT message [junji torii] 7478e86
* bugfix:cas command result was corrected in forward process [junji torii] c84e00d
* add mail subject prefix feature [byplayer] afcaf48
* add mailer path option to roma watch configuration file . [byplayer] ee997e1
* add the program for watching a ROMA [Muga Nishizawa] ee916a8
* bugfix:after the first node does join, lost vnodes cannot be detected. [junji torii] 2a5b3b2
* refactor:exclusive operation for iteration in the storage. [junji torii] 7f7d27c
* bugfix:stop by irregular data in storage_clean_up_process [junji torii] 40ba9bb
* add sample_watcher3, which is a checker for split brain [muga] 749f444
* bugfix:exception happens when nodes is nil has fixed. [junji torii] 1982e72
* refactor:improve a version command result. [junji torii] 24dd282
* bugfix:exclusive operation for iteration in the storage. [junji torii] 03e468e
* added a recoverlost_alist_all. [junji torii] 5cb8dde
* bugfix:for multi hash feature. [junji torii] b065555
* storage commands was moved to plugin. [junji torii] ff3de38
* add dependency to eventmachine . [byplayer] f1b0efc
* bugfix:eventual consistency in the delete command. [junji torii] bf5eecb
* added a bin option in the routingdump command. [junji torii] 47ad0b7
* --config option was added to the argument of romad. [junji torii] 6e6cee5
* bugfix:multihash concerning the file path was corrected. [junji torii] 0a6f63f
* Daemonization was isolated from class of Romad. [junji torii] 4a786fb
* fixed bug for a recover command option. [junji torii] b847b4d
* the forward operation was corrected in the get command. [junji torii] e18019a
* improved a vnode balance parameter. [junji torii] 40c762c
* Bug was fixed that auto-termination logic when my node doesn't exist in the routing-table. [junji torii] 4300767

## 0.8.2 (Jan 6 2010)

* cron.rb removed. [junji torii] dce07ab
* added a showbalance command. [junji torii] 0094287
* supported a cas command. [junji torii] cee0f96
* improved a gets result for a cas-id. [junji torii] 54e136b
* get_raw method was added. [junji torii] e005407
* improve a log level setting. [junji torii] ab97ead
* supported a gets command in ruby client. [junji torii] b553b8b

## 0.8.1 (Nov 26 2009)

* update version. [junji torii] c9267c5
* supported a gets command. [junji torii] 8301706
* fixes the bugs of tribunus [Shumpei Akai] da31dca
* added simple_bench.rb [junji torii] 130a5c9
* Improve a command option. [junji torii] 74e3b66
* Merge roma gem branch into master [byplayer] 9c5f023
* fixed bug [junji torii] 9a10943
* fixed bug [junji torii] c3ea85a
* use FileUtils.rm_rf. [Kouhei Sutou] 141c525
* added a set_gap_for_failover command. [junji torii] 364be47
* added a debug plugin. [junji torii] 2908e4e
* don't log to STDOUT. [Kouhei Sutou] 78fb6b2
* Javaのソースコード：インデントを修正 [TAKANO Mitsuhiro] 74eed3d
* add package dir and doc dir to .gitignore . [byplayer] 9d559ac
* add gem package task . [byplayer] 4cb31b2
* 少しRubyらしく書き換えた些細な修正と正規表現の誤りを修正 [TAKANO Mitsuhiro] 24c016d
* ファイルごとにコーディングスタイルをあわせるなど [TAKANO Mitsuhiro] 9bfefdd
* 不要な式展開を変数に置き換えた [TAKANO Mitsuhiro] 48c3f20
* puts was replaced with logger. [junji torii] 45b03b2
* use assert_raise for exception check. [Kouhei Sutou] a18cd0d
* added a prototype of roma client proxy daemon. [junji torii] 14b68e8
* remove needless test name display. [Kouhei Sutou] f234ee2
* fixed bug in t_rclient.rb. [junji torii] 68880b5
* support running test with Ruby not named as 'ruby'. [Kouhei Sutou] 6fd1ec2

## 0.8.0 (Oct 23 2009)
