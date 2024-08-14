# Connection Pool

## Overview

The load-balancing algorithm is designed to optimize the allocation and
management of database connections in a way that maximizes Quality of Service
(QoS). This involves minimizing the overall time spent on connecting and
reconnecting (connection efficiency) while ensuring that latencies remain
similar across different streams of connections (fairness).

## Architecture

This library is split into four major components:

 1. The low-level blocks/block, connections, and metrics code. This code
    creates, destroys and transfers connections without understanding of
    policies, quotas or any sort of algorithm. We ensure that the blocks and
    metrics are reliable, and use this as a building block for our pool.
 2. The algorithm. This performs planning operations for acquisition, release
    and rebalancing of the pool. The algorithm does not perform operations, but
    rather informs that caller what it should do.
 3. The pool itself. This drives the blocks and the connector interface, and
    polls the algorithm to plan next steps during acquisition, release and
    during the timer-based planning callback.
 4. The Python integration code. This is behind an optional feature, and exposes
    PyO3-based interface that allows a connection factory to be implemented in
    Python.

## Details

Demand for connections is measured in terms of “database time,” which is
calculated as the product of the number of connections and the average hold time
of these connections. This metric provides a basis for determining how resources
should be distributed among different database blocks to meet their needs
effectively.

To maximize QoS, the algorithm aims to minimize the time spent on managing
connections and keep the latencies low and uniform across various connection
streams. This involves allocation strategies that balance the immediate needs of
different database blocks with the overall system capacity and future demand
predictions.

When a connection is acquired, the system may be in a state where the pool is
not currently constrained by demand. In such cases, connections can be allocated
greedily without complex balancing, as there are sufficient resources to meet
all demands. This allows for quick connection handling without additional
overhead.

When the pool is constrained, the “stealing” algorithm aims to transfer
connections from less utilized or idle database blocks (victims) to those
experiencing high demand (hunger) to ensure efficient resource use and maintain
QoS. A victim block is chosen based on its idle state, characterized by holding
connections but having low or no immediate demand for them.

Upon releasing a connection, the algorithm evaluates which backend (database
block) needs the connection the most (the hungriest). This decision is based on
current demand, wait times, and historical usage patterns. By reallocating
connections to the blocks that need them most, the algorithm ensures that
resources are utilized efficiently and effectively.

Unused connection capacity is eventually reclaimed to prevent wastage. The
algorithm includes mechanisms to identify and collect these idle connections,
redistributing them to blocks with higher demand or returning them to the pool
for future use. This helps maintain an optimal number of active connections,
reducing unnecessary resource consumption.

To avoid excessive thrashing, the algorithm ensures that connections are held
for a minimum period, which is longer than the time it takes to reconnect to a
database or a configured minimum threshold. This reduces the frequency of
reallocation, preventing performance degradation due to constant connection
churn and ensuring that blocks can maintain stable and predictable access to
resource

## Detailed Algorithm

The algorithm is designed to 1) maximize time spent running queries in a
database and 2) minimize latency of queries waiting for their turn to run. These
goals may be in conflict at times. We do this by optimizing the time spent
switching between databases, which is considered "dead time" -- as the database
is not actively performing operations.

The demand for a connection is based on estimated total sequential processing
time. We use the average time that a connection is held, times the number of
connections in demand as a rough idea of how much total sequential time a
certain block demands in the future.

At a regular interval, we compute two items for each block: a quota, and a
"hunger" metric. The hunger metric may indicate that a block is "hungry"
(wanting more connections), satisfied (having the expected number of
connections) or overfull (holding more connections than it should). The "hungry"
score is determined by the estimated total sequential time needed for a block.
The "overfull" score is determined by the number of extra connections held by
this block, in combination with how old the longest-held connection is. Quota is
determined by the connection rate.

We then use the hunger metric and quota in an attempt to rebalance the pool
proactively to ensure that the connection capacity of each block reflects its
most recent demand profile. Blocks are sorted into a list of hungry and overfull
blocks, and we attempt to transfer from the most hungry to the most overfull
until we run out of either list. We may not be able to perform the rebalance
fully because of block activity that cannot be interrupted.

If a connection is requested for a block that is hungry, it is allowed to steal
a connection from the block that most overfull and has idle connections. As the
"overfull" score is calculated in part by the longest-held connection's age, we
minimize context switching.

When a connection is released, we choose what happens based on its state. If
more connections are waiting on this block, we return the connection to the
block to be re-used immediately. If no connections are waiting but the block is
hungry, we return it. If the block is satisfied or overfull and we have hungry
blocks waiting, we transfer it to a hungry block that has waiters.

## Error Handling

The pool will attempt to provide a connection where possible, but connection
operations may not always be reliable. The error for a connection failure will
be routed through the acquire operation if the pool detects there are no other
potential sources for a connection for the acquire. Sources for a connection may
be a currently-connecting connection, a reconnecting connection, a connection
that is actively held by someone else or a connection that is sitting idle.

The pool does not currently retry, and retry logic should be included in the
connect operation.
