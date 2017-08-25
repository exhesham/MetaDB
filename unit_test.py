import os
from metadb_api import MetaDB

print
print "Testing find/insert/find_one/remove basic"
print "=============================================="
mdb = MetaDB()
mdb.database.remove({})
print "Empty after remove.....", mdb.database.find({}) == []
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'c'})
mdb.database.insert({'a':'d'})
print "After inserting 3 values.....",sorted(mdb.database.find({})) == sorted([{'a':'d'}, {'a':'c'}, {'a':'b'}])
print "Find with filter.....", mdb.database.find({'a':'d'}) == [{'a':'d'}]
print
print "Testing find/insert/find_one/remove advanced"
print "=============================================="
mdb = MetaDB()
mdb.database.remove({})
print "Empty after remove.....",mdb.database.find({})==[]
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'c'})
mdb.database.insert({'a':'d'})
print "Find with filter replications.....", mdb.database.find({'a':'b'}) == [{'a':'b'},{'a':'b'},{'a':'b'},{'a':'b'},]
mdb.database.remove({'a':'b'})
print "After deleting by felter 4 similar values.....",sorted(mdb.database.find({})) == sorted([{'a':'d'}, {'a':'c'}])
mdb.database.remove({})
print "Empty after remove.....",mdb.database.find({})==[]

print
print "Testing update/find_one"
print "=============================================="
mdb = MetaDB()
mdb.database.remove({})
print "Empty after remove.....", mdb.database.find({}) == []
mdb.database.insert({'a':'b'})
mdb.database.update({'a':'b'},{'a':'b2', 'c':'d'})
print "Find filter with new value.....", mdb.database.find({'a':'b2'}) == [{'a':'b2', 'c':'d'}]
print "Find filter with old value.....", mdb.database.find({'a':'b'}) == []
mdb.database.insert({'a':'b'})
mdb.database.update({'a':'b'},{'a':'b', 'k':'l'})
mdb.database.insert({'a':'b'})
print "After deleting by felter 4 similar values.....",sorted(mdb.database.find({'a':'b'})) == sorted([{'a':'b'}, {'a':'b', 'k':'l'}])
print "find_one_1", mdb.database.find_one({'a':'b'}) == {'a':'b', 'k':'l'}
print "find_one_1", mdb.database.find_one({'k':'l'}) == {'a':'b', 'k':'l'}


print
print "Testing find empty"
print "=============================================="
mdb = MetaDB()
mdb.database.remove({})
print "Empty after remove.....", mdb.database.find({}) == []
print "Empty after remove.....", mdb.database.find_one({'a','b'}) == None
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'b'})
mdb.database.insert({'a':'c'})
mdb.database.insert({'a':'d'})

mdb.database.update({}, {'a2':'d2'}, upsert=True)
print "Find update all.....", mdb.database.find({}), mdb.database.find({}) == [{'a2':'d2'}]