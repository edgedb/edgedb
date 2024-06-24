# Connection Pool

## Overview

The load-balancing algorithm is designed to optimize the allocation and
management of database connections in a way that maximizes Quality of Service
(QoS). This involves minimizing the overall time spent on connecting and
reconnecting while ensuring that latencies remain similar across different
streams of connections.

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

The “stealing” algorithm aims to transfer connections from less utilized or idle
database blocks (victims) to those experiencing high demand (hunger) to ensure
efficient resource use and maintain QoS. A victim block is chosen based on its
idle state, characterized by holding connections but having low or no immediate
demand for them.

Upon releasing a connection, the algorithm evaluates which backend (database
block) needs the connection the most (the hungriest). This decision is based on
current demand, wait times, and historical usage patterns. By reallocating
connections to the blocks that need them most, the algorithm ensures that
resources are utilized efficiently and effectively.

Unused connection capacity should be reclaimed to prevent wastage. The algorithm
includes mechanisms to identify and collect these idle connections,
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


