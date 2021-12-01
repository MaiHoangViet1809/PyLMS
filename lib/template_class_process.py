import hashlib
import json
import time
import uuid
import lib.connector_db as db
from setting import DEBUG_MODE


class setting:
    max_id_per_topic = 50000


class generator:
    @classmethod
    def hash(cls, value):
        return hashlib.sha256(value)

    @classmethod
    def uuid(cls):
        return uuid.uuid4().hex


class Connector:
    def __init__(self):
        # _connector
        self.database = None

    def connect_db(self):
        self.database = db.database()

    def disconnect_db(self):
        self.database = None


class base:
    framework_var_list = ['is_dirty', '_list_dirty', '_done_init', '_connector', 'track_change']

    def __init__(self, json_data: str = None, topic: str = None):
        self._connector = Connector()

        # track change
        self._list_dirty = {}
        self.is_dirty = False

        # topic queue
        self.topic_name = topic

        # value attribute
        if json_data:
            dict_data = json.loads(json_data)
            for key in dict_data:
                if key not in self.__dict__:
                    self.__setattr__(key, dict_data[key])
                else:
                    if self.__dict__[key] is None:
                        self.__setattr__(key, dict_data[key])

        # flag to stop add attribute after init
        self._done_init = True

    def lock(self):
        self._done_init = True

    def unlock(self):
        self.__delattr__('_done_init')

    def __setattr__(self, attr, val):
        if hasattr(self, '_done_init'):
            if attr not in self.__dict__:
                raise TypeError("can not add more attribute to {} after init !!".format(self))
            else:
                self.__dict__[attr] = val
                if attr not in base.framework_var_list:
                    super().__setattr__('is_dirty', True)
                    lists = super().__getattribute__('_list_dirty')
                    lists[attr] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    super().__setattr__('_list_dirty', lists)
        else:
            super().__setattr__(attr, val)

    async def commit(self, is_track_change: bool = False):
        duration_dict = {}
        start_time = time.time()

        if not self.is_dirty:  # clear track change if not flag as dirty
            self._list_dirty = {}
        try:
            object_id, id_value = next((k, v) for k, v in self.contract_info.items() if k.endswith('loan_id'))
        except Exception as E:
            print("commit error find object_id:", E)

        if is_track_change:
            d = {"msg_no": generator.uuid(),
                 **{key: self.__dict__[key] for key in self.__dict__
                    if key not in base.framework_var_list
                    and not key.startswith("_")
                    },
                 "track_change": {object_id: id_value,
                                  **{key: (self.__dict__[key], self._list_dirty[key])
                                     for key in self._list_dirty
                                     if key not in base.framework_var_list
                                     }}
                 }
        else:
            d = {"msg_no": generator.uuid(),
                 **{key: self.__dict__[key] for key in self.__dict__
                    if key not in base.framework_var_list
                    and not key.startswith("_")
                    }
                 }

        if self.is_dirty:  # clear flag dirty when done
            self._update()

        result = json.dumps(d)
        if DEBUG_MODE > 0:
            duration_dict['commit: prepare json'], start_time = round(time.time() - start_time, 4), time.time()

        # update topic index into db
        tool = topic_index()
        for attr in self.__dict__:
            if '_id' in attr:
                tool.upsert_index(entity_name=attr.replace('_id', ''),
                                  entity_id=self.__dict__[attr],
                                  topic_name=self.topic_name)
        if DEBUG_MODE > 0:
            duration_dict['update id in database'], start_time = round(time.time() - start_time, 4), time.time()

        if DEBUG_MODE > 0:
            print(duration_dict)
        return result

    def _update(self):
        self.is_dirty = False
        self._list_dirty = {}


class topic_index:
    def __init__(self):
        # _connector attribute
        self.connector = Connector()

    def upsert_index(self, entity_name: str, entity_id: str, topic_name: str):
        self.connector.connect_db()
        sql = '''INSERT OR REPLACE INTO TBL_INDEX_{} ({}_id, topic_name) values ('{}', '{}')
              '''.format(entity_name, entity_name, entity_id, str(topic_name).upper())
        self.connector.database.exec(sql_text=sql)
        self.connector.disconnect_db()

    def available_topic(self, entity_name: str):
        self.connector.connect_db()
        sql = '''SELECT TOPIC_NAME, COUNT(1) NUM 
                   FROM TBL_INDEX_{}
               GROUP BY TOPIC_NAME
                 HAVING COUNT(1) < {}
              '''.format(entity_name, setting.max_id_per_topic)
        for row in self.connector.database.exec(sql_text=sql):
            topic_name = row[0]
            self.connector.disconnect_db()
            return str(topic_name).upper()

        sql = '''SELECT COUNT(1) C
                   FROM TBL_INDEX_{}'''.format(entity_name)
        for row in self.connector.database.exec(sql_text=sql):
            if row[0] == 0:
                topic_name = entity_name + '_1'
                self.connector.disconnect_db()
                return str(topic_name).upper()
            else:
                sql = '''SELECT SUBSTR(MAX(TOPIC_NAME)
                                      ,1
                                      ,INSTR(MAX(TOPIC_NAME),'_')-1
                                      )
                             ||'_'
                             || (CAST(SUBSTR(MAX(TOPIC_NAME)
                                      ,INSTR(MAX(TOPIC_NAME),'_')+1
                                      ,LENGTH(MAX(TOPIC_NAME))
                                      ) AS INTEGER) + 1)
                                AS TOPIC_NAME
                           FROM TBL_INDEX_{}
                    '''.format(entity_name)
                for r in self.connector.database.exec(sql_text=sql):
                    topic_name = r[0]
                    self.connector.disconnect_db()
                    return str(topic_name).upper()

    def find_by_key(self, entity_name: str, entity_id: str = None):
        self.connector.connect_db()
        topic_name = None
        sql = '''SELECT TOPIC_NAME 
                   FROM TBL_INDEX_{} 
                  WHERE {}_id = '{}'
              '''.format(entity_name, entity_name, entity_id)
        for row in self.connector.database.exec(sql_text=sql):
            topic_name = row[0]
            break
        self.connector.disconnect_db()
        return str(topic_name).upper()
