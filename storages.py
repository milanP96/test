import json
import os
import re


class BaseStorage:
    def insert(self, key, value):
        raise NotImplementedError('Method not implemented!')

    def select(self, key):
        raise NotImplementedError('Method not implemented!')

    def update(self, key, value):
        raise NotImplementedError('Method not implemented!')

    def delete(self, key):
        raise NotImplementedError('Method not implemented!')

    def exists(self):
        raise NotImplementedError('Method not implemented!')


class JsonStorage(BaseStorage):
    """Storage will work with dictionary"""

    def __init__(self):
        if not os.path.isfile('json_data.json'):
            self.memory = dict()
        else:
            self.load_from_disk()
            
        self.prevent_write = False
        
        self.transactions = list()
        self.to_rollback = list()

    def load_from_disk(self):
        self.memory = self.open()
        
    def begin(self):
        self.prevent_write = True
    
    @staticmethod
    def build_rollback_transaction(action, data):
        if action == 'update':
            return dict(action=action, value=data)
        
        if action == 'delete':
            return dict(action='insert', value=data)
        
        if action == 'insert':
            return dict(action='delete', value=data)

    def rollback(self):
        self.prevent_write = False
        transactions_last_index = len(self.to_rollback) - 1

        while len(self.to_rollback) > 0:
            rollback_action = self.to_rollback[transactions_last_index]

            try:
                action = getattr(self, rollback_action['action'])

                if isinstance(rollback_action['value'], dict):
                    print(rollback_action['value'])
                    key = next(iter(rollback_action['value']))
                    action(key, rollback_action['value'][key])
                else:
                    action(rollback_action['value'])
            except Exception as e:
                print("Error")
            else:
                self.to_rollback = self.to_rollback[:-1]
                transactions_last_index = len(self.to_rollback) - 1


        self.prevent_write = True

    def commit(self):
        self.prevent_write = False

        for transaction in self.transactions:
            rollback_value = None
            rollback_transaction = None
            
            try:
                self.load_from_disk()
                
                if transaction['action'] == 'update':
                    key = next(iter(transaction['value']))
                    rollback_value = {key: self.select(key)}

                if transaction['action'] == 'delete':
                    key = transaction['value']
                    rollback_value = {key: self.select(key)}

                if transaction['action'] == 'insert':
                    rollback_value = next(iter(transaction['value']))
                    
                rollback_transaction = self.build_rollback_transaction(
                    transaction['action'],
                    rollback_value
                )

                action = getattr(self, transaction['action'])

                if isinstance(transaction['value'], dict):
                    key = next(iter(transaction['value']))
                    action(key, transaction['value'][key])
                    print(transaction['action'], transaction['value'])
                else:
                    print(transaction['action'], transaction['value'])
                    action(transaction['value'])
            except Exception as e:
                print(f"Error: {e}, starting rollback")
                self.rollback()
                break
            else:
                #  If everything is ok save a potential rollback
                self.to_rollback.append(rollback_transaction)

        self.prevent_write = True

    @staticmethod
    def open():
        with open('json_data.json') as json_file:
            data = json.load(json_file)
            return data

    def write(self):
        with open('json_data.json', 'w') as json_file:
            json.dump(self.memory, json_file)

    def insert(self, key, value):
        resp = {key: value}
        if self.prevent_write:
            #  Transaction logic
            self.transactions.append(
                dict(
                    action='insert',
                    value=resp
                )
            )
        else:
            #  First logic
            if self.exists(key):
                raise ValueError(f"Key {key} already exists")
            self.memory[key] = value
            self.write()
        return resp

    def select(self, key):

        if not self.exists(key):
            raise ValueError(f"Key {key} is not in memory, select error")

        return self.memory.get(key)

    def update(self, key, value):

        resp = {key: value}
        if self.prevent_write:
            #  Transaction logic
            self.transactions.append(
                dict(
                    action='update',
                    value=resp
                )
            )
        else:
            #  First logic
            if not self.exists(key):
                raise ValueError(f"Key {key} is not in memory, update error")
            self.memory[key] = value
            self.write()
        
        return resp

    def delete(self, key):
        if self.prevent_write:
            #  Transaction logic
            self.transactions.append(
                dict(
                    action='delete',
                    value=key
                )
            )
        else:
            #  First logic
            if not self.exists(key):
                raise ValueError("This key is not in memory, delete error")
            del self.memory[key]
            self.write()

    def exists(self, key):
        return key in self.memory

    def keys(self, expression=None):
        if expression is None:
            return list(self.memory.keys())

        reg = re.compile(expression)

        return list(filter(reg.search, list(self.memory.keys())))


class DictStorage(BaseStorage):
    """Storage will work with dictionary"""

    def __init__(self):
        self.memory = dict()

    def insert(self, key, value):
        if self.exists(key):
            raise ValueError("This key already exists")

        self.memory[key] = value
        return {key: value}

    def select(self, key):
        if not self.exists(key):
            raise ValueError("This key is not in memory")

        return self.memory.get(key)

    def update(self, key, value):
        if not self.exists(key):
            raise ValueError("This key is not in memory")

        self.memory[key] = value
        return {key: value}

    def delete(self, key):
        if not self.exists(key):
            raise ValueError("This key is not in memory")

        del self.memory[key]

    def exists(self, key):
        return key in self.memory
