package com;

public class Info {
    /**
     * ***** put操作
     *     shell 格式
     *  1。----插入一条数据
     *       1）常用的 put shell 操作：
     *         put 'tablename', 'rowkey','cf:column','value','timestamp'
     *         hbase> put 'ns1:t1', 'r1', 'c1', 'value'
               hbase> put 't1', 'r1', 'c1', 'value'
               hbase> put 't1', 'r1', 'c1', 'value', ts1
               hbase> put 't1', 'r1', 'c1', 'value', {ATTRIBUTES=>{'mykey'=>'myvalue'}}
               hbase> put 't1', 'r1', 'c1', 'value', ts1, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
               hbase> put 't1', 'r1', 'c1', 'value', ts1, {VISIBILITY=>'PRIVATE|SECRET'}

                The same commands also can be run on a table reference. Suppose you had a reference
                   t to table 't1', the corresponding command would be:

                hbase> t.put 'r1', 'c1', 'value', ts1, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
             2) 插入的数据有版本的空控制，更新操作是再次 put 对应的行健，列族，列限定符和新的数据更新数据，
                 其中历史数据会被保存下来，查看需要通过指定对应的版本。
                    scan 't1', {RAW => true, VERSIONS => 10}
     *    2,---get 操作获取数据
     *    Here is some help for this command:
          Get row or cell contents; pass table name, row, and optionally
            a dictionary of column(s), timestamp, timerange and versions. Examples:
            hbase> get 'ns1:t1', 'r1'
            hbase> get 't1', 'r1'
            hbase> get 't1', 'r1', {TIMERANGE => [ts1, ts2]}
            hbase> get 't1', 'r1', {COLUMN => 'c1'}
            hbase> get 't1', 'r1', {COLUMN => ['c1', 'c2', 'c3']}
            hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1}
            hbase> get 't1', 'r1', {COLUMN => 'c1', TIMERANGE => [ts1, ts2], VERSIONS => 4}
            hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, VERSIONS => 4}
            hbase> get 't1', 'r1', {FILTER => "ValueFilter(=, 'binary:abc')"}
            hbase> get 't1', 'r1', 'c1'
            hbase> get 't1', 'r1', 'c1', 'c2'
            hbase> get 't1', 'r1', ['c1', 'c2']
            hbase> get 't1', 'r1', {COLUMN => 'c1', ATTRIBUTES => {'mykey'=>'myvalue'}}
            hbase> get 't1', 'r1', {COLUMN => 'c1', AUTHORIZATIONS => ['PRIVATE','SECRET']}
            hbase> get 't1', 'r1', {CONSISTENCY => 'TIMELINE'}
            hbase> get 't1', 'r1', {CONSISTENCY => 'TIMELINE', REGION_REPLICA_ID => 1}



         Besides the default 'toStringBinary' format, 'get' also supports custom formatting by
            column.  A user can define a FORMATTER by adding it to the column name in the get
            specification.  The FORMATTER can be stipulated:

            1. either as a org.apache.hadoop.hbase.util.Bytes method name (e.g, toInt, toString)
            2. or as a custom class followed by method name: e.g. 'c(MyFormatterClass).format'.

           Example formatting cf:qualifier1 and cf:qualifier2 both as Integers:
           hbase> get 't1', 'r1' {COLUMN => ['cf:qualifier1:toInt',
           'cf:qualifier2:c(org.apache.hadoop.hbase.util.Bytes).toInt'] }

           Note that you can specify a FORMATTER by column only (cf:qualifier).  You cannot specify
           a FORMATTER for all columns of a column family.

           The same commands also can be run on a reference to a table (obtained via get_table or
           create_table). Suppose you had a reference t to table 't1', the corresponding commands
            would be:

           hbase> t.get 'r1'
           hbase> t.get 'r1', {TIMERANGE => [ts1, ts2]}
           hbase> t.get 'r1', {COLUMN => 'c1'}
           hbase> t.get 'r1', {COLUMN => ['c1', 'c2', 'c3']}
           hbase> t.get 'r1', {COLUMN => 'c1', TIMESTAMP => ts1}
           hbase> t.get 'r1', {COLUMN => 'c1', TIMERANGE => [ts1, ts2], VERSIONS => 4}
           hbase> t.get 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, VERSIONS => 4}
           hbase> t.get 'r1', {FILTER => "ValueFilter(=, 'binary:abc')"}
           hbase> t.get 'r1', 'c1'
           hbase> t.get 'r1', 'c1', 'c2'
           hbase> t.get 'r1', ['c1', 'c2']
           hbase> t.get 'r1', {CONSISTENCY => 'TIMELINE'}
           hbase> t.get 'r1', {CONSISTENCY => 'TIMELINE', REGION_REPLICA_ID => 1

         3,请空表
               方法一 truncate <table
               方法二：具体过程：disable table -> drop table -> create table
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     */

}
