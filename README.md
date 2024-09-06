This is a Rustified version of daos ``client apis. The structures for pools, containers, objects are in src/daos.rs. The operations of objects are in src/daos_obj_ops.rs. There are two groups of operations for objects. One is synchronous. The other is asynchronous. Check the tests for examples.

In order to support asynchronous operations DAOS requires a thread to drive tse progress. I create a thread for each event queue to drive tse progress. The objects in a container share one event queue. This event queue is a field in DaosContainer.
