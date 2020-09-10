# 基于scala的实现版本说明

## 最新的处理策略 (version-2.0)

1. 在每个分区内部建立三维R树（R树的索引如果太大，可以先按照数据量进行切分），R树中的每个点的结构如下：

   ```
   {
   	id;
   	timeOffset;
   	lon;
   	lat;
   }
   ```

2. 输入查询后，在每个分区内部进行单点多次查询，每次的查询范围相当于一个面。

3. 实现将文件按照每条轨迹的第一个点进行Z-Order排序，保证读入数据块时，可以优先将附近范围的点全部读入。