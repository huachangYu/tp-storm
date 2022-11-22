package org.apache.storm.executor.strategy;

public enum StrategyType {
    Fair, OnlyQueue, QueueAndCost, QueueAndWait, QueueAndCostAndWait, AD
}

