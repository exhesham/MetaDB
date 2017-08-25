# Meta-Data Database

## Goal

Storing metadata usually done via mongodb as it support data-recovery and resilient. However, for some applications, there
is a downside for using mongo:

1. Mongo is not part of your service
2. Deleting mongo will result in your meta-data loss
3. It is a bigger solution for a smaller problem
4. Data replication is not a community feature

By using this opensource, the metaDB will be part of your application as it will ease the migration from mongo to its API.
Thanks to the fact that it uses the same API and design pattern.


## Concurrency

TBD

## Failure Propagating

TBD

