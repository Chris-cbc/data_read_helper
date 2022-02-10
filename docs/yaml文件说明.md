yaml文件说明

```
version: "1.0"
environment: spark				# 指定运行的应用，如果用spark就不用修改
dependency_file: True			# 指定spark的依赖包，不指定填False
pipeline:
  reader:
    name: HiveReader			# reader的脚本名字，如果用hive读就不用修改
    account:
      host: 172.16.20.130
      port: 10000
      username: bigdata
      password: bigdata@253
      database: "cl_cdm"
    sql: "select submit_msg_id, client_msg_id, mobile, signature, send_time, ptt_mon, content from cl_cdm.dwd_dms_msg_detial_di where ptt_mon = 201911 limit 2"
  writer:
    name: ClickHouseWriter		# writer脚本的名字，如果用clickhouse就不用修改
    account:
      host: 172.16.20.45
      port: 9000
      username: default
      password: ""
      database: "cl_cdm"
    target_table: "dwd_dms_msg_detial_di_remove_dup_cluster"
    error_table:


script: this_is_a_demo			# 要运行的脚本名字
tasks:
  - task: demo					# 脚本里面的函数名字
    in: [content]				# demo函数的输入字段名字
    out: sms_template_1			# demo函数的输出字段名字
    ignore: true
  - task: demo
    in: [sms_template_1]
    out: sms_template_2

# 需要存表里的字段，字段顺序要和建表字段顺序一致
output: ["submit_msg_id", "client_msg_id", "mobile",
         "signature", "send_time", "ptt_mon", "sms_template_2"]
```