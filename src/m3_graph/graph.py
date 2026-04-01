import os, asyncio
from pathlib import Path

from collections import defaultdict
from typing import Type, ClassVar

from psycopg.types.json import Jsonb
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

        if create:
            await cls._create_schema(conn, schema)

        graph = cls(conn, schema)

        if create:
            await graph.maintain()

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
    async def _create_schema(cls, conn: DBConn, name: str = 'catalog'):
        """Create the database schema (tables, triggers, functions) without instantiating a Graph."""
        for file in sorted(cls._sql_folder_path.glob('*.sql')):
            sql_def = file.read_text().replace('{name}', name)
            await conn.execute(sql_def)

    @classmethod
    async def create(cls, conn: DBConn, name: str = 'catalog'):
        await cls._create_schema(conn, name)

        graph = cls(conn, name)
        await graph.maintain()

        return graph
    
    async def maintain(self):
        graph_cls = type(self)
        # Use the registered type/subtype dicts so we cover the full hierarchy
        # (not just direct subclasses) and avoid cross-test pollution via the
        # persistent Python class hierarchy.
        classes = set(graph_cls.types.values()) | set(graph_cls.subtypes.values())
        await asyncio.gather(*(db_cls.maintain() for db_cls in classes))
    
    def transaction(self):
        """Context manager for explicit database transactions."""
        return self._conn.transaction()

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
            # If already loaded, remove old index entries before replacing
            if row['id'] in self.registry:
                self.registry[row['id']]._remove_from_indexes()

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

    async def _bulk_insert(self, objects: list[DBObject]):
        """Insert multiple DBObjects in a single multi-row INSERT statement.

        This triggers the statement-level trigger once for the entire batch,
        enabling set-based backlink maintenance instead of per-row.
        """
        if not objects:
            return

        for obj in objects:
            if obj.id is not None:
                raise ValueError("Cannot insert object with existing id")

        # Build rows for execute_many-style insert, but we need RETURNING
        # so we use a single INSERT with unnest arrays
        categories = [obj.category for obj in objects]
        types = [obj.type for obj in objects]
        subtypes = [obj.subtype for obj in objects]
        attrs = [Jsonb(obj._get_attr()) for obj in objects]
        sources = [obj.source for obj in objects]

        rows = await self._conn.query(
            f"""
            INSERT INTO {self._schema}.object (category, type, subtype, attr, source)
            SELECT unnest(%(categories)s::text[]),
                   unnest(%(types)s::text[]),
                   unnest(%(subtypes)s::text[]),
                   unnest(%(attrs)s::jsonb[]),
                   unnest(%(sources)s::text[])
            RETURNING id
            """,
            categories=categories,
            types=types,
            subtypes=subtypes,
            attrs=attrs,
            sources=sources,
        )

        # Assign IDs and register
        for obj, row in zip(objects, rows):
            obj.id = row['id']
            self.registry[obj.id] = obj
            if hasattr(obj, 'type') and obj.type:
                self.registry_type[obj.type][obj.id] = obj
            obj._update_indexes()
            obj._clear_all_unsaved_backlinks()
            obj._convert_backlink_refs_to_ids()

    async def _bulk_upsert(self, objects: list[DBObject]):
        """Upsert multiple objects, resolving dependency order automatically.

        Topologically sorts objects by their unsaved references so parents
        are inserted before children. Each dependency layer is bulk-inserted
        in a single statement. Objects that already have an id are updated
        individually.
        """
        if not objects:
            return

        to_update = [o for o in objects if o.id is not None]
        to_insert = [o for o in objects if o.id is None]

        # Topological sort: partition into layers by dependency depth
        remaining = set(id(o) for o in to_insert)
        obj_map = {id(o): o for o in to_insert}
        layers: list[list[DBObject]] = []

        while remaining:
            # A layer is all objects whose unsaved refs are NOT in remaining
            layer = []
            for oid in list(remaining):
                obj = obj_map[oid]
                unsaved = getattr(obj, '_unsaved_refs', {})
                deps_in_remaining = any(
                    id(ref) in remaining for ref in unsaved.values()
                )
                if not deps_in_remaining:
                    layer.append(obj)

            if not layer:
                names = [repr(obj_map[oid]) for oid in list(remaining)[:3]]
                raise ValueError(
                    f"Circular unsaved references among {len(remaining)} objects "
                    f"(e.g. {', '.join(names)})"
                )

            for obj in layer:
                remaining.discard(id(obj))
            layers.append(layer)

        # Insert layer by layer
        for layer in layers:
            # Resolve FK ids from now-saved refs
            for obj in layer:
                unsaved = getattr(obj, '_unsaved_refs', None)
                if not unsaved:
                    continue
                for rel_name, ref in list(unsaved.items()):
                    if ref.id is not None:
                        setattr(obj, f"{rel_name}_id", ref.id)
                        obj._clear_unsaved_backlinks(rel_name)
                        del unsaved[rel_name]

            await self._bulk_insert(layer)

        # Updates (no batching — update_object checks per row)
        for obj in to_update:
            # Resolve any unsaved refs
            unsaved = getattr(obj, '_unsaved_refs', None)
            if unsaved:
                for rel_name, ref in list(unsaved.items()):
                    if ref.id is not None:
                        setattr(obj, f"{rel_name}_id", ref.id)
                        obj._clear_unsaved_backlinks(rel_name)
                        del unsaved[rel_name]
            await self._update(obj)

    async def _update(self, obj: DBObject) -> bool:
        """Update existing DBObject in database; returns True only when data actually changed."""
        if obj.id is None:
            raise ValueError("Cannot update object without id")

        rows = await self._conn.query(
            f"SELECT {self._schema}.update_object(%(id)s, %(category)s, %(type)s, %(subtype)s, %(attr)s, %(source)s) AS changed",
            id=obj.id,
            category=obj.category,
            type=obj.type,
            subtype=obj.subtype,
            attr=Jsonb(obj._get_attr()),
            source=obj.source,
        )
        return rows[0]['changed']

    async def _delete(self, obj: DBObject) -> bool:
        """Delete object from database and remove from registries; always returns True."""
        if obj.id is None:
            raise ValueError("Cannot delete object without id")

        await self._conn.execute(f"DELETE FROM {self._schema}.object WHERE id = %(id)s", id=obj.id)

        obj._remove_from_indexes()

        self.registry.pop(obj.id, None)
        if hasattr(obj, 'type') and obj.type:
            self.registry_type[obj.type].pop(obj.id, None)

        obj.id = None
        return True
