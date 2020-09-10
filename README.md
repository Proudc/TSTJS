# Temporal-Spatial Trajectory Join

## dataFormat

一些实验设置和使用到的数据结构

## index

目前只有三维RTree索引

## method/Baseline.scala

最暴力的实现方法，全部遍历

## method/SpaceTimePar.scala

当前基于时空分区的实现方法

## method/BaseZorder.scala

进行时间分区，每个分区内部的轨迹按照z-order的顺序排序

## method/oldVersion

之前比较老的实现版本，先进行filter，之后进行refine

## selfPartitioner

一些使用到的自定义分区方法