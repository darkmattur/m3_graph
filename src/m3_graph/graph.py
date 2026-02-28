import os, asyncio
from pathlib import Path

from collections import defaultdict
from typing import Type, ClassVar

from .conn import connect, DBConn
from .object import DBObject


class classproperty:
    """Descriptor for class-level property with caching."""
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __get__(self, obj, owner):
        if owner not in self.cache:
            self.cache[owner] = self.func(owner)
        return self.cache[owner]

class Graph:
    """
    Graph instance that maintains type registries, object caches, and indexes.
    Each Graph instance provides isolated object management and caching.
    """
    
    # Class-level registries
    types: ClassVar[dict[str, Type]]
    subtypes: ClassVar[dict[str, Type]]

    # Instance Registries
    registry: dict[int, object]
    registry_type: defaultdict[str, dict[int, object]]

    @classproperty
    def DBObject(cls) -> Type[DBObject]:
        """Returns a DBObject subclass with this Graph class attached."""
        class GraphedDBObject(DBObject):
            _graph_cls = cls

        return GraphedDBObject
    
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Type registries
        cls.types = dict()
        cls.subtypes = dict()
    
    @classmethod
    async def connect(
        cls,
        host: str, dbname: str, schema: str = 'catalog', port: int | None = None,
        user: str | None = None, password: str | None = None, create: bool = False,
        **kwargs
    ):
        conn = await connect(
            host=host, port=port, dbname=dbname,
            user=user, password=password, **kwargs
        )
        graph = cls(conn, schema)

        if create:
            await graph.create(conn, schema)
        
        return graph

    
    def __init__(self, conn: DBConn, schema: str = 'catalog'):
        """
        Initialize a Graph instance.

        Args:
            conn: Database connection wrapper with query/execute methods
            name: Name of the schema
        """
        if type(self) is Graph:
            raise TypeError('Graph class cannot be initialised direcly!')

        self._conn = conn
        self._schema = schema

        # Object registries
        self.registry = dict()
        self.registry_type = defaultdict(dict)

        # Set graph instance on DBObject base class so all subclasses can access it
        self.DBObject.graph = self

        for subtype in self.subtypes.values():
            setattr(self, subtype.__name__, subtype)

    ####################################################################################
    # Graph utility methods
    ####################################################################################

    _sql_folder_path = Path(os.path.dirname(__file__)) / 'sql'

    @classmethod
    async def create(cls, conn: DBConn, name: str = 'catalog'):
        for file in sorted(cls._sql_folder_path.glob('*.sql')):
            sql_def = file.read_text().replace('{name}', name)
            await conn.execute(sql_def)
        
        graph = cls(conn, name)
        await graph.maintain()

        return graph
    
    async def maintain(self):
        await asyncio.gather(*(
            db_cls.maintain() for db_cls in self.DBObject.__subclasses__()
        ))
    
    ####################################################################################
    # Internal Methods
    ####################################################################################

    # DBObject hooks

    @classmethod
    def _object_register_cls(cls, db_object_cls):
        if "type" in db_object_cls.__dict__ and db_object_cls.type:
            if db_object_cls.type in cls.types and db_object_cls is not cls.types[db_object_cls.type]:
                raise ValueError(f"'{db_object_cls.type}' already registered, type names must be unique")
            cls.types[db_object_cls.type] = db_object_cls

        if hasattr(db_object_cls, 'subtype') and db_object_cls.subtype:
            if db_object_cls.subtype in cls.subtypes and db_object_cls is not cls.subtypes[db_object_cls.subtype]:
                raise ValueError(f"'{db_object_cls.subtype}' already registered, subtype names must be unique")
            cls.subtypes[db_object_cls.subtype] = db_object_cls

    # Database utilities

    async def load(self):
        """Load all objects from database into this graph."""
        # Load all objects from the database in one query
        rows = await self._conn.query(
            f"SELECT id, category, type, subtype, attr, source FROM {self._schema}.object ORDER BY id"
        )

        # Initialize objects one by one using the subtype registry
        for row in rows:
            subtype = row['subtype']

            # Get the appropriate class for this subtype
            if subtype in self.subtypes:
                cls = self.subtypes[subtype]
            elif row['type'] in self.types:
                cls = self.types[row['type']]
            else:
                # Skip objects with unregistered types
                continue

            # Create the object with all its attributes
            obj_data = {
                'id': row['id'],
                'source': row['source'],
                **row['attr']
            }
            
            # Initialize the object
            cls(**obj_data)
    
    async def _insert(self, obj: DBObject):
        """Insert new DBObject into database."""
        if obj.id is not None:
            raise ValueError("Cannot insert object with existing id")

        result = await self._conn.query(
            f"""
            INSERT INTO {self._schema}.object (category, type, subtype, attr, source)
            VALUES (%(category)s, %(type)s, %(subtype)s, %(attr)s, %(source)s)
            RETURNING id
            """,
            category=obj.category, type=obj.type, subtype=obj.subtype,
            attr=obj._get_attr(), source=obj.source
        )

        obj.id = result[0]['id']

        # Register the object in registries
        self.registry[obj.id] = obj
        if hasattr(obj, 'type') and obj.type:
            self.registry_type[obj.type][obj.id] = obj
        obj._update_indexes()

        return obj.id
    
    async def _update(self, obj: DBObject):
        """Update existing DBObject in database."""
        if obj.id is None:
            raise ValueError("Cannot update object without id")

        await self._conn.execute(
            f"""
            UPDATE {self._schema}.object
            SET category = %(category)s, type = %(type)s, subtype = %(subtype)s, attr = %(attr)s, source = %(source)s
            WHERE id = %(id)s
            """,
            id=obj.id,
            category=obj.category,
            type=obj.type,
            subtype=obj.subtype,
            attr=obj._get_attr(),
            source=obj.source
        )
    
    async def _delete(self, obj: DBObject):
        """Delete object from database and remove from registries."""
        if obj.id is None:
            raise ValueError("Cannot delete object without id")

        await self._conn.execute(f"DELETE FROM {self._schema}.object WHERE id = %(id)s", id=obj.id)

        self.registry.pop(obj.id, None)
        if hasattr(obj, 'type') and obj.type:
            self.registry_type[obj.type].pop(obj.id, None)

        obj.id = None
