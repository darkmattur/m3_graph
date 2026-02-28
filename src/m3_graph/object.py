import itertools
from typing import Type, get_type_hints, ClassVar, TYPE_CHECKING

from pydantic import BaseModel, Field

from .util import unwrap_optional, is_optional
from .link import LinkInfo, BacklinkInfo, extract_link_info

if TYPE_CHECKING:
    from .graph import Graph


class DBObject(BaseModel):

    ####################################################################################
    # Configurable
    ####################################################################################

    # Class Attributes
    _graph_cls: ClassVar[Type['Graph']]
    graph: ClassVar['Graph']

    # @property
    # def graph(self) -> 'Graph':
    #     """Access the graph instance for this object."""
    #     return self.__class__._graph_instance

    # Typing
    category: ClassVar[str]
    type: ClassVar[str]
    subtype: ClassVar[str]

    # Attributes excluded from DB storage
    excluded_attrs: ClassVar[set[str]] = set()

    # Uniqueness constraints, indexes for fast lookups
    category_unique_attr: ClassVar[list[tuple[str, ...] | str]] = []
    type_unique_attr: ClassVar[list[tuple[str, ...] | str]] = []
    subtype_unique_attr: ClassVar[list[tuple[str, ...] | str]] = []
    computed_unique_attr: ClassVar[list[str]] = []  # Computed properties (in-memory only, not in DB)

    ####################################################################################
    # Internal
    ####################################################################################

    # Instance Attributes
    id: int | None = None
    source: str | None = None

    # Relationships
    _forward_rels: ClassVar[dict[str, tuple[str | None, bool]]] = {}
    _back_rels: ClassVar[set[str]] = set()

    _category_indexes: ClassVar[dict[tuple[str, ...], dict]] = {}
    _type_indexes: ClassVar[dict[tuple[str, ...], dict]] = {}
    _subtype_indexes: ClassVar[dict[tuple[str, ...], dict]] = {}
    _computed_indexes: ClassVar[dict[str, dict]] = {}
    
    def __init__(self, **data):
        # Track unsaved object references
        unsaved_refs = {}

        # Handle relationship assignments during initialization
        for key in list(data.keys()):
            if key.endswith('_id') or key.endswith('_ids'):
                continue

            if key in self._forward_rels:
                value = data.pop(key)
                id_key = f"{key}_id"

                key_type, key_nullable = self._forward_rels[key]
                if value is None:
                    if not key_nullable:
                        raise ValueError(f"'{key}' is required and cannot be None")
                    data[id_key] = None
                elif isinstance(value, DBObject):
                    if value.id is None:
                        # Store reference to unsaved object
                        unsaved_refs[key] = value
                        data[id_key] = None
                    else:
                        data[id_key] = int(value.id)
                else:
                    data[id_key] = int(value)

        super().__init__(**data)

        # Store unsaved references
        self._unsaved_refs = unsaved_refs

        # Register instance if id is set
        if self.id:
            self.graph.registry[self.id] = self
            if hasattr(self, 'type') and self.type:
                self.graph.registry_type[self.type][self.id] = self
            self._update_indexes()

    def __setattr__(self, name: str, value):
        """Override setattr to automatically update indexes when indexed attributes change."""
        # Get all indexed attribute names
        indexed_attrs = set()
        for cols in itertools.chain(
            self._category_indexes.keys(),
            self._type_indexes.keys(),
            self._subtype_indexes.keys()
        ):
            indexed_attrs.update(cols)

        # If this is an indexed attribute and the object is already initialized
        if name in indexed_attrs and hasattr(self, 'id') and self.id is not None:
            # Remove old index entries before changing the value
            self._remove_from_indexes()
            # Change the value
            super().__setattr__(name, value)
            # Add new index entries with the new value
            self._update_indexes()
        else:
            # Normal attribute setting
            super().__setattr__(name, value)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        try:
            type_hints = get_type_hints(cls, include_extras=True)
        except NameError:
            # Fall back to raw annotations if forward references can't be resolved
            type_hints = {}
        annotations: dict = getattr(cls, '__annotations__', {})

        # 1. Relationships
        forward_rels = {}
        back_rels = set()

        for base in cls.__mro__[1:]:
            forward_rels.update(getattr(base, '_forward_rels', {}))
            back_rels.update(getattr(base, '_back_rels', set()))

        for name in list(annotations.keys()):
            annotation = type_hints.get(name, annotations[name])

            link_info = extract_link_info(annotation)
            if link_info is None:
                continue

            # Remove the relationship annotation and any default value
            annotations.pop(name, None)
            if name in cls.__dict__:
                delattr(cls, name)

            # Handle Links
            if isinstance(link_info, LinkInfo):
                id_field = f"{name}_id"
                nullable = is_optional(annotation)

                linked_type = unwrap_optional(link_info.target)
                assert issubclass(linked_type, DBObject)

                # Always allow None to support unsaved object references
                annotations[id_field] = int | None
                setattr(cls, id_field, None)
                forward_rels[name] = (link_info.backlink, nullable)

                def make_getter(field, rel_name):
                    def getter(self):
                        # Check for unsaved reference first
                        if hasattr(self, '_unsaved_refs') and rel_name in self._unsaved_refs:
                            return self._unsaved_refs[rel_name]

                        foreign_key = getattr(self, field, None)
                        if foreign_key is None:
                            return None
                        return self.graph.registry.get(foreign_key)
                    return getter

                def make_setter(field, rel_name, is_nullable):
                    def setter(self, value):
                        if value is None:
                            if not is_nullable:
                                raise ValueError(f"{rel_name} is required and cannot be None")
                            setattr(self, field, None)
                            # Clear unsaved reference
                            if hasattr(self, '_unsaved_refs') and rel_name in self._unsaved_refs:
                                del self._unsaved_refs[rel_name]
                        elif isinstance(value, DBObject):
                            if value.id is None:
                                # Store unsaved object reference
                                if not hasattr(self, '_unsaved_refs'):
                                    self._unsaved_refs = {}
                                self._unsaved_refs[rel_name] = value
                                setattr(self, field, None)
                            else:
                                # Backfill the registry if the object has an ID
                                if value.id not in self.graph.registry:
                                    self.graph.registry[value.id] = value
                                    if hasattr(value, 'type') and value.type:
                                        self.graph.registry_type[value.type][value.id] = value
                                setattr(self, field, int(value.id))
                                # Clear unsaved reference
                                if hasattr(self, '_unsaved_refs') and rel_name in self._unsaved_refs:
                                    del self._unsaved_refs[rel_name]
                        elif isinstance(value, int):
                            setattr(self, field, value)
                            # Clear unsaved reference
                            if hasattr(self, '_unsaved_refs') and rel_name in self._unsaved_refs:
                                del self._unsaved_refs[rel_name]
                        else:
                            raise ValueError(f"Cannot assign non-DBObject to {rel_name}")
                    return setter

                setattr(cls, name, property(make_getter(id_field, name), make_setter(id_field, name, nullable)))

            elif isinstance(link_info, BacklinkInfo):
                ids_field = f"{name}_ids"
                back_rels.add(ids_field)

                annotations[ids_field] = list[int]
                setattr(cls, ids_field, Field(default_factory=list, exclude=True))

                def getter(self: DBObject):
                    return [
                        self.graph.registry[id_] for id_ in (getattr(self, ids_field, []) or [])
                        if id_ in self.graph.registry
                    ]

                setattr(cls, name, property(getter))

        cls.__annotations__ = annotations
        cls._forward_rels = forward_rels
        cls._back_rels = back_rels

        # 2. Set defaults
        if 'type' in cls.__dict__ and 'subtype' not in cls.__dict__:
            cls.subtype = cls.type

        # 3. Initialize indexes
        cls._category_indexes = {}
        cls._type_indexes = {}
        cls._subtype_indexes = {}
        cls._computed_indexes = {}

        # Collect constraints from inheritance chain
        cls._all_category_constraints = list({c for base in reversed(cls.__mro__)
            if hasattr(base, 'category_unique_attr') for c in base.category_unique_attr})
        cls._all_type_constraints = list({c for base in reversed(cls.__mro__)
            if hasattr(base, 'type_unique_attr') for c in base.type_unique_attr})
        cls._all_subtype_constraints = list({c for base in reversed(cls.__mro__)
            if hasattr(base, 'subtype_unique_attr') for c in base.subtype_unique_attr})
        cls._all_computed_constraints = list({c for base in reversed(cls.__mro__)
            if hasattr(base, 'computed_unique_attr') for c in base.computed_unique_attr})

        # Initialize index dictionaries
        for attribute, constraints, indexes_attr in [
            ('category', '_all_category_constraints', '_category_indexes'),
            ('type', '_all_type_constraints', '_type_indexes'),
            ('subtype', '_all_subtype_constraints', '_subtype_indexes')
        ]:
            if not hasattr(cls, attribute) or getattr(cls, attribute) is None:
                continue

            indexes = getattr(cls, indexes_attr)
            for c in getattr(cls, constraints, []):
                key = (c,) if isinstance(c, str) else c
                indexes[key] = {}

        # Initialize computed indexes
        if hasattr(cls, '_all_computed_constraints'):
            for prop_name in cls._all_computed_constraints:
                cls._computed_indexes[prop_name] = {}

        # Register type
        # Skip the synthetic GraphedDBObject class created by the descriptor
        if cls.__name__ not in {'GraphedDBObject', 'DBObject'}:
            cls._graph_cls._object_register_cls(cls)

    # Index Management

    def _update_indexes(self):
        """Update all unique indexes with this instance."""
        for cols, idx in itertools.chain(
            self._category_indexes.items(), self._type_indexes.items(), self._subtype_indexes.items()
        ):
            key = tuple(getattr(self, col) for col in cols)
            idx[key] = self

        # Update computed property indexes
        for prop_name, idx in self._computed_indexes.items():
            value = getattr(self, prop_name, None)
            if value is not None:
                idx[value] = self
    
    def _remove_from_indexes(self):
        """Remove this instance from all unique indexes."""
        for cols, idx in self._category_indexes.items():
            key = tuple(getattr(self, col, None) for col in cols)
            idx.pop(key, None)

        for cols, idx in self._type_indexes.items():
            key = tuple(getattr(self, col, None) for col in cols)
            idx.pop(key, None)

        for cols, idx in self._subtype_indexes.items():
            key = tuple(getattr(self, col, None) for col in cols)
            idx.pop(key, None)

        # Remove from computed property indexes
        for prop_name, idx in self._computed_indexes.items():
            value = getattr(self, prop_name, None)
            if value is not None:
                idx.pop(value, None)
    
    ####################################################################################
    # Database Methods
    ####################################################################################

    @classmethod
    async def load(cls, expand: bool = False):
        """Load all objects of this type (including inherited subtypes) into the graph.

        Args:
            expand: If True, also load all objects connected via relationships (forward and backward)

        Returns:
            List of loaded objects
        """
        if not hasattr(cls, 'type') or cls.type is None:
            raise ValueError(f"Cannot load objects for class {cls.__name__} without a type attribute")

        # Use SQL function to fetch objects by type (with optional expansion)
        rows = await cls.graph._conn.query(
            f"SELECT * FROM {cls.graph._schema}.fetch_object_by_type(%(type)s, %(expand)s)",
            type=cls.type,
            expand=expand
        )

        # Load objects into the registry
        loaded_objects = []
        graph_cls = type(cls.graph)

        for row in rows:
            # Skip if already loaded
            if row['id'] in cls.graph.registry:
                loaded_objects.append(cls.graph.registry[row['id']])
                continue

            subtype = row['subtype']

            # Get the appropriate class for this subtype
            if subtype in graph_cls.subtypes:
                obj_cls = graph_cls.subtypes[subtype]
            elif row['type'] in graph_cls.types:
                obj_cls = graph_cls.types[row['type']]
            else:
                # Skip objects with unregistered types
                continue

            # Create the object with all its attributes
            obj_data = {
                'id': row['id'],
                'source': row['source'],
                **row['attr']
            }

            # Initialize the object (this registers it automatically)
            obj = obj_cls(**obj_data)
            loaded_objects.append(obj)

        return loaded_objects

    @classmethod
    def all(cls):
        """Get all objects of this type currently in the graph registry.

        Returns:
            List of objects of this type
        """
        if not hasattr(cls, 'type') or cls.type is None:
            return []

        # Get objects from the type-specific registry
        return list(cls.graph.registry_type.get(cls.type, {}).values())

    @classmethod
    def get(cls, **kwargs):
        """Retrieve a single object by unique attributes or computed properties.

        Uses in-memory indexes for fast lookups. Searches in order:
        1. Computed property indexes (for single-property lookups)
        2. Subtype unique constraints (most specific)
        3. Type unique constraints
        4. Category unique constraints (least specific)

        Args:
            **kwargs: Attribute values to search for

        Returns:
            The matching object

        Raises:
            ValueError: No unique constraint matches provided kwargs
            KeyError: No object found matching the query

        Example:
            user = User.get(email="user@example.com")
            asset = Asset.get(symbol="BTC")
        """
        if not kwargs:
            raise ValueError("At least one keyword argument required")

        # Check computed property indexes first (single property lookups)
        if len(kwargs) == 1:
            prop_name = next(iter(kwargs.keys()))
            if prop_name in cls._computed_indexes:
                value = kwargs[prop_name]
                obj = cls._computed_indexes[prop_name].get(value)
                if obj is None:
                    raise KeyError(f"No {cls.__name__} found with {prop_name}={value}")
                return obj

        # Check subtype, type, then category indexes (most specific to least specific)
        for cols, idx in itertools.chain(
            cls._subtype_indexes.items(),
            cls._type_indexes.items(),
            cls._category_indexes.items()
        ):
            if set(cols) == set(kwargs.keys()):
                key = tuple(kwargs[col] for col in cols)
                obj = idx.get(key)
                if obj is None:
                    raise KeyError(f"No {cls.__name__} found with {kwargs}")
                return obj

        # No matching unique constraint
        computed = list(cls._computed_indexes.keys())
        raise ValueError(
            f"No unique constraint for {set(kwargs.keys())}. "
            f"Available: subtype={list(cls._subtype_indexes.keys())}, "
            f"type={list(cls._type_indexes.keys())}, "
            f"category={list(cls._category_indexes.keys())}"
            + (f", computed={computed}" if computed else "")
        )

    @classmethod
    def filter(cls, **kwargs):
        """Filter objects by attributes (linear search through in-memory objects).

        Use .get() for unique lookups with indexes for better performance.

        Args:
            **kwargs: Attribute values to filter by

        Returns:
            List of matching objects

        Example:
            active_users = User.filter(status="active")
            recent_tasks = Task.filter(status="pending", priority="high")
        """
        if not kwargs:
            return cls.all()

        return [
            obj for obj in cls.all()
            if all(getattr(obj, k, None) == v for k, v in kwargs.items())
        ]

    def _get_attr(self) -> dict:
        """Extract attribute dict for database storage."""
        # Use mode='python' to preserve Decimal objects (don't serialize to JSON yet)
        data = self.model_dump(mode='python', exclude={"id", "source"} | self.excluded_attrs)
        
        # Filter out None values, keep forward relationship fields (_id), exclude backlinks (_ids)
        # Backlink arrays are managed by database triggers, not the ORM
        result = {
            k: v for k, v in data.items()
            if (k.endswith('_id') or v is not None) and not k.endswith('_ids')
        }

        return result
    
    async def insert(self):
        """Insert this object into the database."""
        if self.id is not None:
            raise ValueError("Cannot insert object with existing id")

        # Check for unsaved references - require explicit saving
        if hasattr(self, '_unsaved_refs') and self._unsaved_refs:
            unsaved_names = ', '.join(self._unsaved_refs.keys())
            raise ValueError(
                f"Cannot insert object with unsaved references: {unsaved_names}. "
                f"Either save them first or use upsert() for automatic cascading save."
            )

        self.id = await self.graph._insert(self)

    async def update(self):
        """Update this object in the database."""
        if self.id is None:
            raise ValueError("Cannot update object without id")

        # Check for unsaved references - require explicit saving
        if hasattr(self, '_unsaved_refs') and self._unsaved_refs:
            unsaved_names = ', '.join(self._unsaved_refs.keys())
            raise ValueError(
                f"Cannot update object with unsaved references: {unsaved_names}. "
                f"Either save them first or use upsert() for automatic cascading save."
            )

        await self.graph._update(self)
    
    async def upsert(self):
        """Insert or update this object in the database.

        Automatically upserts any unsaved related objects first.
        """
        # Upsert any unsaved related objects first (cascading save)
        if hasattr(self, '_unsaved_refs') and self._unsaved_refs:
            for rel_name, related_obj in list(self._unsaved_refs.items()):
                await related_obj.upsert()
                # Update the ID field
                id_field = f"{rel_name}_id"
                setattr(self, id_field, related_obj.id)
                # Remove from unsaved refs
                del self._unsaved_refs[rel_name]

        if self.id is None:
            await self.insert()
        else:
            await self.update()

    async def delete(self):
        """Delete this object from the database and remove it from the graph."""
        if self.id is None:
            raise ValueError("Cannot delete object without id")

        await self.graph._delete(self)
    
    # Maintenance

    @classmethod
    async def _register_relationships(cls):
        """Store relationship and inheritance metadata in the database."""
        if not (hasattr(cls, 'category') and hasattr(cls, 'type') and hasattr(cls, 'subtype')):
            return
        if cls.category is None or cls.type is None or cls.subtype is None:
            return

        # Collect parent types from base classes
        parent_types = []
        for base in cls.__mro__[1:]:
            if hasattr(base, 'type') and base.type and base.type != cls.type:
                parent_types.append(base.type)

        # Convert forward_rels to use _id suffix for database storage
        # ORM stores: {"author": ("books", False)}
        # DB needs: {"author_id": "books_ids"}
        forward_for_db = {}
        for rel_name, (backlink_name, nullable) in cls._forward_rels.items():
            id_field = f"{rel_name}_id"
            if backlink_name:
                backlink_ids_field = f"{backlink_name}_ids"
                forward_for_db[id_field] = backlink_ids_field
            else:
                forward_for_db[id_field] = None

        await cls.graph._conn.execute(
            f"""
            INSERT INTO {cls.graph._schema}.meta (category, type, subtype, forward, back, parent_types, descendant_types)
            VALUES (%(category)s, %(type)s, %(subtype)s, %(forward)s, %(back)s, %(parent_types)s, ARRAY[%(type)s])
            ON CONFLICT (category, type, subtype)
            DO UPDATE SET
                forward = EXCLUDED.forward,
                back = EXCLUDED.back,
                parent_types = EXCLUDED.parent_types,
                descendant_types = ARRAY[%(type)s]
            """,
            category=cls.category,
            type=cls.type,
            subtype=cls.subtype,
            forward=forward_for_db if forward_for_db else None,
            back=list(cls._back_rels) if cls._back_rels else None,
            parent_types=parent_types if parent_types else [],
        )

    @classmethod
    async def _create_unique_index(cls):
        """Create database unique indexes for this type."""
        for attribute, constraints in [
            ('category', cls.category_unique_attr),
            ('type', cls.type_unique_attr),
            ('subtype', cls.subtype_unique_attr)
        ]:
            attribute_value = getattr(cls, attribute, None)
            if attribute_value is None:
                continue

            for constraint in constraints:
                cols = (constraint,) if isinstance(constraint, str) else constraint
                idx_name = f"idx_unique_{attribute_value}_{'_'.join(cols)}"
                expr = f"((attr->>'{cols[0]}'))" if len(cols) == 1 else ", ".join(f"(attr->>'{c}')" for c in cols)

                await cls.graph._conn.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
                    ON {cls.graph._schema}.object ({expr})
                    WHERE {attribute} = '{attribute_value}'
                """)

    @classmethod
    async def maintain(cls):
        """Perform database maintenance tasks for this type."""
        await cls._register_relationships()
        await cls._create_unique_index()
