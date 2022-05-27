from sdk import StorageSDK


db = StorageSDK('json_storage')

insert_1 = db.storage.insert('key', 'value')
print(insert_1)

select_1 = db.storage.select('key')
print(select_1)

update_1 = db.storage.update('key', 'new_value')
print(update_1)

db.storage.delete('key')

# select_2 = db.storage.select('key')
# print(select_2)

# Transactions

db = StorageSDK('json_storage')
db.storage.begin()

insert_1 = db.storage.insert('key2', 'value')
insert_2 = db.storage.insert('key3', 'value')
insert_3 = db.storage.insert('key4', 'value')

update_1 = db.storage.update('key2', 'new_value')

db.storage.delete('key')
db.storage.commit()

print(db.storage.keys(r'^.*ey4.*$'), "Keys")
