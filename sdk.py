from storages import BaseStorage, DictStorage, JsonStorage


class StorageSDK:
    def __init__(self, storage_key):
        storage_class = self.get_storage_class(storage_key)
        self.storage = storage_class()

    @staticmethod
    def get_storage_class(key: str) -> BaseStorage:
        """Function will return storage class by predefined key"""

        storage_map = dict(
            dict_storage=DictStorage,
            json_storage=JsonStorage
        )

        if key not in storage_map.keys():
            raise ValueError('Requested storage is not implemented')

        return storage_map.get(key)
