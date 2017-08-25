#!/usr/bin/env python
import os
import sys
import json
import logging

logging.raiseExceptions = False
logging.basicConfig(filename=file('metaDB.log'),
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.DEBUG)
logger = logging.getLogger('metadb')


def excepthook(type_, value, traceback):
    logger.exception(value)
    sys.__excepthook__(type_, value, traceback)


DATABASE_NAME = '/etc/metadata/__metadata'

class MetaDB:
    def __getattr__(self, name):
        self.__dict__[name] = MetaDB.MetaTable(name)
        return self.__dict__[name]

    def __init__(self):
        try:
            metadb_dir = os.path.dirname(DATABASE_NAME)
            if not os.path.exists(metadb_dir):
                logger.info("Creating the directory %s " % metadb_dir)
                os.makedirs(metadb_dir)
        except:
            logger.error("failed to create the directory:%s" % metadb_dir)
            return 1

    @staticmethod
    def _get_metafile_of_table(table_name):
        return '{0}.{1}.json'.format(DATABASE_NAME, table_name)

    @staticmethod
    def _write_to_json_file( table_name, json_data):
        logger.info("Writing to json %s %s" % (table_name, json_data))
        with open(MetaDB._get_metafile_of_table(table_name), "w+") as text_file:
            text_file.write(json.dumps(json_data))
            return text_file.close()
        return 1

    @staticmethod
    def _read_json_file(table_name):
        logger.info("reading from json  table %s" % (table_name))
        text = '[]'
        if not os.path.exists(MetaDB._get_metafile_of_table(table_name)):
            logger.info((" file ", MetaDB._get_metafile_of_table(table_name), " doesn't exist! will not read it!"))
            return text

        text_file = open(MetaDB._get_metafile_of_table(table_name), "r")
        text = text_file.read()

        return json.loads(text)



    class MetaTable:
        def __init__(self, db_name):
            self.db_name = db_name

        def fulfill_filter(self, record, filter):
            if not record:
                return False
            for key in filter:
                if not key in record or not record.get(key) == filter.get(key):
                    return False
            return True

        def find(self, filter):
            table_json = MetaDB._read_json_file(self.db_name)
            return [record for record in table_json if self.fulfill_filter(record, filter)]

        def iterate(self, filter):
            logger.info("iterate")
            table_json = MetaDB._read_json_file(self.db_name)
            return (record for record in table_json if self.fulfill_filter(record, filter))

        def find_one(self, filter):
            logger.info("find_one")
            generator = self.iterate(filter)
            try:
                nextval = next(generator)
            except:
                return None

            return nextval

        def remove(self, filter):
            logger.info("remove")
            table_json = MetaDB._read_json_file(self.db_name)
            legit_records = [record for record in table_json if not self.fulfill_filter(record, filter)]
            return MetaDB._write_to_json_file(self.db_name, legit_records)

        def insert(self, data):
            logger.info("insert")
            table_json = MetaDB._read_json_file(self.db_name)
            table_json.append(data)
            return MetaDB._write_to_json_file(self.db_name, table_json)

        def update(self, filter, data, upsert=False):
            logger.info("update")
            updated_jsons = []
            json_old_arr = self.find(filter)
            json_new = data

            if not upsert:
                for json_old in json_old_arr:
                    dictA = json_old
                    dictB = json_new

                    merged_dict = {key: value for (key, value) in (dictA.items() + dictB.items())}

                    updated_jsons.append(merged_dict)
            else:
                updated_jsons.append(data)

            table_json = MetaDB._read_json_file(self.db_name)
            legit_records = [record for record in table_json if not self.fulfill_filter(record, filter)]
            legit_records.extend(updated_jsons)
            return MetaDB._write_to_json_file(self.db_name, legit_records)