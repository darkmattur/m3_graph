import re
import sys
import itertools
from typing import Type, get_type_hints, ClassVar, TYPE_CHECKING

from pydantic import BaseModel, Field

from .util import unwrap_optional, is_optional
from .link import LinkInfo, BacklinkInfo, extract_link_info

if TYPE_CHECKING:
    from .graph import Graph


def _parse_link_from_str(annotation: str) -> 'LinkInfo | BacklinkInfo | None':
    """Parse a raw string annotation to extract LinkInfo or BacklinkInfo.

    Used as a fallback when get_type_hints() fails because the referenced class
    lives in a local (function) scope and cannot be resolved via module globals.
    """
    s = annotation.strip()
    # Strip Optional / union-with-None wrappers
    s = re.sub(r'\s*\|\s*None', '', s).strip()
    s = re.sub(r'None\s*\|\s*', '', s).strip()
    m = re.match(r'^Optional\[(.+)\]$', s)
    if m:
        s = m.group(1).strip()

    # Link[Target] or Link[Target, "backlink"] or Link[Target, 'backlink']
    m = re.match(r"^Link\[(\w+)(?:,\s*['\"](\w+)['\"]\s*)?\]$", s)
    if m:
        return LinkInfo(target=None, backlink=m.group(2))

    # Backlink[Target]
    if re.match(r'^Backlink\[(\w+)\]$', s):
        return BacklinkInfo()

    return None


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
    _forward_rel_types: ClassVar[dict[str, Type['DBObject']]] = {}
    _back_rels: ClassVar[set[str]] = set()

    _category_indexes: ClassVar[dict[tuple[str, ...], dict]] = {}
    _type_indexes: ClassVar[dict[tuple[str, ...], dict]] = {}
    _subtype_indexes: ClassVar[dict[tuple[str, ...], dict]] = {}
    _computed_indexes: ClassVar[dict[str, dict]] = {}
    
    def __init__(self, **data):
        # Track unsaved object references and saved object relationships
        unsaved_refs = {}
        saved_rels = {}  # {rel_name: saved_target_obj}

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
                        # Track saved object relationships for backlink registration
                        saved_rels[key] = value
                else:
                    data[id_key] = int(value)

        super().__init__(**data)

        # Store unsaved references
        self._unsaved_refs = unsaved_refs

        # Register backlinks for unsaved references
        for rel_name, related_obj in unsaved_refs.items():
            backlink_name, _ = self._forward_rels[rel_name]
            if backlink_name:
                self._register_unsaved_backlink(related_obj, backlink_name, self)

        # Register backlinks for saved relationships (when self is unsaved)
        if self.id is None:
            for rel_name, related_obj in saved_rels.items():
                backlink_name, _ = self._forward_rels[rel_name]
                if backlink_name:
                    self._register_unsaved_backlink(related_obj, backlink_name, self)

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

        annotations: dict = getattr(cls, '__annotations__', {})

        # Include the class itself in localns so self-referencing annotations
        # (e.g. parent: Link[EVMChain] on EVMChain) resolve during class creation.
        localns = {cls.__name__: cls}

        try:
            type_hints = get_type_hints(cls, localns=localns, include_extras=True)
        except NameError:
            # get_type_hints failed (likely due to forward refs to local-scope classes).
            # Try evaluating each annotation individually so non-link fields still resolve.
            module = sys.modules.get(cls.__module__, None)
            globalns = vars(module) if module else {}
            type_hints = {}
            for attr_name, annotation_str in annotations.items():
                if not isinstance(annotation_str, str):
                    type_hints[attr_name] = annotation_str
                    continue
                try:
                    type_hints[attr_name] = eval(annotation_str, globalns, localns)
                except NameError:
                    pass  # Unresolvable forward ref – will be parsed as string below

        # 1. Relationships
        forward_rels = {}
        forward_rel_types = {}
        back_rels = set()

        for base in cls.__mro__[1:]:
            forward_rels.update(getattr(base, '_forward_rels', {}))
            forward_rel_types.update(getattr(base, '_forward_rel_types', {}))
            back_rels.update(getattr(base, '_back_rels', set()))

        for name in list(annotations.keys()):
            annotation = type_hints.get(name, annotations[name])

            link_info = extract_link_info(annotation)
            # If the annotation couldn't be resolved (stays as a string due to a
            # forward reference to a local-scope class), parse the string directly.
            if link_info is None and isinstance(annotation, str):
                link_info = _parse_link_from_str(annotation)
            if link_info is None:
                continue

            # Remove the relationship annotation and any default value
            annotations.pop(name, None)
            if name in cls.__dict__:
                delattr(cls, name)

            # Handle Links
            if isinstance(link_info, LinkInfo):
                id_field = f"{name}_id"
                if isinstance(annotation, str):
                    nullable = bool(
                        re.search(r'\|\s*None|None\s*\|', annotation)
                        or annotation.startswith('Optional[')
                    )
                else:
                    nullable = is_optional(annotation)

                # Only validate the target type when it has been resolved.
                if link_info.target is not None:
                    linked_type = unwrap_optional(link_info.target)
                    assert issubclass(linked_type, DBObject)
                    forward_rel_types[name] = linked_type

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
                        # Get backlink name for this relationship
                        backlink_name, _ = self._forward_rels.get(rel_name, (None, None))

                        # Remove old backlink if exists (check current value)
                        current_id = getattr(self, field, None)
                        if backlink_name and current_id is None:
                            # If current ID is None, might have unsaved ref
                            if hasattr(self, '_unsaved_refs') and rel_name in self._unsaved_refs:
                                old_target = self._unsaved_refs[rel_name]
                                self._unregister_unsaved_backlink(old_target, backlink_name, self)

                        if value is None:
                            if not is_nullable:
                                raise ValueError(f"{rel_name} is required and cannot be None")
                            # Remove old backlink if self is unsaved and target was saved
                            if backlink_name and self.id is None and current_id is not None:
                                old_target = self.graph.registry.get(current_id)
                                if old_target:
                                    self._unregister_unsaved_backlink(old_target, backlink_name, self)
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
                                # Register backlink on target object
                                if backlink_name:
                                    self._register_unsaved_backlink(value, backlink_name, self)
                                # Remove old backlink if it was to a saved object
                                if current_id is not None and self.id is None:
                                    old_target = self.graph.registry.get(current_id)
                                    if old_target:
                                        self._unregister_unsaved_backlink(old_target, backlink_name, self)
                            else:
                                # Backfill the registry if the object has an ID
                                if value.id not in self.graph.registry:
                                    self.graph.registry[value.id] = value
                                    if hasattr(value, 'type') and value.type:
                                        self.graph.registry_type[value.type][value.id] = value
                                setattr(self, field, int(value.id))
                                # If self is unsaved, register backlink on the saved target
                                if backlink_name and self.id is None:
                                    self._register_unsaved_backlink(value, backlink_name, self)
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

                # Store both IDs (int) and unsaved object references (DBObject)
                # Note: We can't use proper typing here since DBObject may not be fully defined yet
                # The list will contain either int or DBObject instances
                annotations[ids_field] = list
                setattr(cls, ids_field, Field(default_factory=list, exclude=True))

                def make_backlink_getter(field):
                    def getter(self: DBObject):
                        items = getattr(self, field, []) or []
                        result = []
                        for item in items:
                            if isinstance(item, int):
                                # Saved object - look up in registry
                                if item in self.graph.registry:
                                    result.append(self.graph.registry[item])
                            elif isinstance(item, DBObject):
                                # Unsaved object - use directly
                                result.append(item)
                        return result
                    return getter

                setattr(cls, name, property(make_backlink_getter(ids_field)))

        cls.__annotations__ = annotations
        cls._forward_rels = forward_rels
        cls._forward_rel_types = forward_rel_types
        cls._back_rels = back_rels

        # 2. Set defaults
        if 'type' in cls.__dict__ and 'subtype' not in cls.__dict__:
            cls.subtype = cls.type

        # 3. Collect excluded_attrs from all parent classes
        cls._all_excluded_attrs = {attr for base in reversed(cls.__mro__)
            if hasattr(base, 'excluded_attrs') for attr in base.excluded_attrs}

        # 4. Initialize indexes
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

    # Unsaved Backlink Management

    @staticmethod
    def _register_unsaved_backlink(target_obj: 'DBObject', backlink_name: str, source_obj: 'DBObject'):
        """Register an unsaved backlink on the target object by adding to _ids list."""
        ids_field = f"{backlink_name}_ids"
        if not hasattr(target_obj, ids_field):
            return

        ids_list = getattr(target_obj, ids_field, [])
        if ids_list is None:
            ids_list = []
            setattr(target_obj, ids_field, ids_list)

        # Avoid duplicates
        if source_obj not in ids_list:
            ids_list.append(source_obj)

    @staticmethod
    def _unregister_unsaved_backlink(target_obj: 'DBObject', backlink_name: str, source_obj: 'DBObject'):
        """Unregister an unsaved backlink from the target object by removing from _ids list."""
        ids_field = f"{backlink_name}_ids"
        if not hasattr(target_obj, ids_field):
            return

        ids_list = getattr(target_obj, ids_field, [])
        if ids_list and source_obj in ids_list:
            ids_list.remove(source_obj)

    def _clear_unsaved_backlinks(self, rel_name: str):
        """Clear unsaved backlinks when a relationship is saved to database."""
        if hasattr(self, '_unsaved_refs') and rel_name in self._unsaved_refs:
            backlink_name, _ = self._forward_rels.get(rel_name, (None, None))
            if backlink_name:
                target = self._unsaved_refs[rel_name]
                self._unregister_unsaved_backlink(target, backlink_name, self)

    def _clear_all_unsaved_backlinks(self):
        """Remove self from targets' in-memory backlink lists when saved.

        The database triggers will handle adding the actual backlink IDs.
        This is an in-memory only feature - we just clean up the unsaved object refs.
        """
        # For all targets this object links to, remove self from their in-memory backlink lists
        for rel_name in self._forward_rels.keys():
            backlink_name, _ = self._forward_rels[rel_name]
            if backlink_name:
                id_field = f"{rel_name}_id"
                target_id = getattr(self, id_field, None)
                if target_id is not None:
                    target = self.graph.registry.get(target_id)
                    if target:
                        self._unregister_unsaved_backlink(target, backlink_name, self)

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
    async def load(cls):
        """Load all objects of this type (including inherited subtypes) and their related objects into the graph registry."""
        if not hasattr(cls, 'type') or cls.type is None:
            raise ValueError(f"Cannot load objects for class {cls.__name__} without a type attribute")

        rows = await cls.graph._conn.query(
            f"SELECT * FROM {cls.graph._schema}.fetch_object_by_type(%(type)s, true)",
            type=cls.type,
        )

        # Load objects into the registry
        graph_cls = type(cls.graph)

        for row in rows:
            # Skip if already loaded
            if row['id'] in cls.graph.registry:
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
            obj_cls(**obj_data)

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
    def find(cls, **kwargs):
        """Retrieve a single object by unique attributes or computed properties.

        Uses in-memory indexes for fast lookups. Searches hierarchically across
        the class and all its subclasses.

        Searches in order:
        1. Computed property indexes (for single-property lookups)
        2. Subtype unique constraints (most specific)
        3. Type unique constraints
        4. Category unique constraints (least specific)

        For each level, searches the current class and all descendant classes.

        Args:
            **kwargs: Attribute values to search for

        Returns:
            The matching object, or None if not found

        Raises:
            ValueError: No unique constraint matches provided kwargs

        Example:
            # Find a specific item (searches Item and all subclasses)
            item = Item.find(code="ABC")  # Returns None if not found

            # Find a specific widget (searches Widget and all subclasses)
            widget = Widget.find(code="XYZ")  # May return SpecialWidget, CustomWidget, etc.
        """
        if not kwargs:
            raise ValueError("At least one keyword argument required")

        # Collect all descendant classes to search hierarchically
        descendant_classes = cls._get_descendant_classes()

        # Check computed property indexes first (single property lookups)
        if len(kwargs) == 1:
            prop_name = next(iter(kwargs.keys()))
            value = kwargs[prop_name]

            # Search current class and all descendants
            for search_cls in descendant_classes:
                if prop_name in search_cls._computed_indexes:
                    obj = search_cls._computed_indexes[prop_name].get(value)
                    if obj is not None:
                        return obj

            # If we got here and computed index exists but no match found
            if any(prop_name in search_cls._computed_indexes for search_cls in descendant_classes):
                return None

        # Check subtype, type, then category indexes (most specific to least specific)
        # Search across all descendant classes for each index level
        # Track if we found matching constraint but no object
        found_matching_constraint = False

        for index_attr in ['_subtype_indexes', '_type_indexes', '_category_indexes']:
            for search_cls in descendant_classes:
                indexes = getattr(search_cls, index_attr)
                for cols, idx in indexes.items():
                    if set(cols) == set(kwargs.keys()):
                        found_matching_constraint = True
                        key = tuple(kwargs[col] for col in cols)
                        obj = idx.get(key)
                        if obj is not None:
                            return obj

        # If we found a matching constraint but no object, return None
        if found_matching_constraint:
            return None

        # No matching unique constraint found - collect available constraints for error message
        all_subtype = set()
        all_type = set()
        all_category = set()
        all_computed = set()

        for search_cls in descendant_classes:
            all_subtype.update(search_cls._subtype_indexes.keys())
            all_type.update(search_cls._type_indexes.keys())
            all_category.update(search_cls._category_indexes.keys())
            all_computed.update(search_cls._computed_indexes.keys())

        raise ValueError(
            f"No unique constraint for {set(kwargs.keys())} in {cls.__name__} or its subclasses. "
            f"Available: subtype={list(all_subtype)}, "
            f"type={list(all_type)}, "
            f"category={list(all_category)}"
            + (f", computed={list(all_computed)}" if all_computed else "")
        )

    @classmethod
    def get(cls, **kwargs):
        """Retrieve a single object by unique attributes or computed properties.

        Like find(), but raises KeyError instead of returning None when no object is found.

        Args:
            **kwargs: Attribute values to search for

        Returns:
            The matching object

        Raises:
            ValueError: No unique constraint matches provided kwargs
            KeyError: No object found matching the query

        Example:
            # Get a specific item (searches Item and all subclasses)
            item = Item.get(code="ABC")  # Raises KeyError if not found

            # Get a specific widget (searches Widget and all subclasses)
            widget = Widget.get(code="XYZ")  # May return SpecialWidget, CustomWidget, etc.
        """
        obj = cls.find(**kwargs)
        if obj is None:
            raise KeyError(f"No {cls.__name__} found with {kwargs}")
        return obj

    @classmethod
    def _get_descendant_classes(cls):
        """Get all descendant classes in the hierarchy, including self.

        Returns classes in order: self first, then all descendants.
        This ensures more specific classes are checked before more general ones.
        """
        descendants = [cls]

        # If we have a graph class with registered subtypes, use it to find descendants
        if hasattr(cls, '_graph_cls') and hasattr(cls._graph_cls, 'subtypes'):
            # Get this class's subtype to find descendants
            if hasattr(cls, 'subtype') and cls.subtype:
                # Find all registered classes that inherit from this class
                for subtype_name, subtype_cls in cls._graph_cls.subtypes.items():
                    if subtype_cls is not cls and issubclass(subtype_cls, cls):
                        descendants.append(subtype_cls)
            elif hasattr(cls, 'type') and cls.type:
                # Use types registry instead
                for type_name, type_cls in cls._graph_cls.types.items():
                    if type_cls is not cls and issubclass(type_cls, cls):
                        descendants.append(type_cls)

        return descendants

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
        data = self.model_dump(mode='python', exclude={"id", "source"} | self._all_excluded_attrs)
        
        # Filter out None values, keep forward relationship fields (_id), exclude backlinks (_ids)
        # Backlink arrays are managed by database triggers, not the ORM
        result = {
            k: v for k, v in data.items()
            if (k.endswith('_id') or v is not None) and not k.endswith('_ids')
        }

        return result
    
    def _convert_backlink_refs_to_ids(self):
        """Convert any saved object references in backlink lists to IDs.

        Keep unsaved object references as-is since they're still valid in-memory backlinks.
        """
        for ids_field in self._back_rels:
            ids_list = getattr(self, ids_field, [])
            if not ids_list:
                continue

            # Convert DBObject references that have IDs to int IDs
            # Keep unsaved DBObject references and int IDs as-is
            converted = []
            for item in ids_list:
                if isinstance(item, DBObject):
                    if item.id is not None:
                        converted.append(item.id)
                    else:
                        # Keep unsaved object reference
                        converted.append(item)
                elif isinstance(item, int):
                    converted.append(item)

            setattr(self, ids_field, converted)

    async def insert(self) -> bool:
        """Insert this object into the database; always returns True."""
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

        # Clear unsaved backlinks now that object is saved
        self._clear_all_unsaved_backlinks()

        # Convert any remaining object references in backlinks to IDs
        self._convert_backlink_refs_to_ids()

        return True

    async def update(self) -> bool:
        """Update this object in the database; returns True only when data actually changed."""
        if self.id is None:
            raise ValueError("Cannot update object without id")

        # Check for unsaved references - require explicit saving
        if hasattr(self, '_unsaved_refs') and self._unsaved_refs:
            unsaved_names = ', '.join(self._unsaved_refs.keys())
            raise ValueError(
                f"Cannot update object with unsaved references: {unsaved_names}. "
                f"Either save them first or use upsert() for automatic cascading save."
            )

        return await self.graph._update(self)

    async def upsert(self) -> bool:
        """Insert or update this object in the database; returns True when data actually changed.

        Automatically upserts any unsaved related objects first.
        """
        # Upsert any unsaved related objects first (cascading save)
        if hasattr(self, '_unsaved_refs') and self._unsaved_refs:
            for rel_name, related_obj in list(self._unsaved_refs.items()):
                await related_obj.upsert()
                # Update the ID field
                id_field = f"{rel_name}_id"
                setattr(self, id_field, related_obj.id)
                # Clear unsaved backlink
                self._clear_unsaved_backlinks(rel_name)
                # Remove from unsaved refs
                del self._unsaved_refs[rel_name]

        if self.id is None:
            return await self.insert()
        else:
            return await self.update()

    async def delete(self) -> bool:
        """Delete this object from the database and remove it from the graph; always returns True."""
        if self.id is None:
            raise ValueError("Cannot delete object without id")

        return await self.graph._delete(self)
    
    # Maintenance

    @classmethod
    async def _register_relationships(cls):
        """Store relationship and inheritance metadata in the database."""
        if not (hasattr(cls, 'category') and hasattr(cls, 'type') and hasattr(cls, 'subtype')):
            return
        if cls.category is None or cls.type is None or cls.subtype is None:
            return

        # Collect parent_types from base classes via MRO
        # NOTE: Despite the name, parent_types stores SUBTYPE values, not type values
        parent_types = []
        for base in cls.__mro__[1:]:
            if hasattr(base, 'subtype') and base.subtype and base.subtype != cls.subtype:
                parent_types.append(base.subtype)

        # Collect descendant_types by checking ALL registered types in the graph
        # NOTE: Despite the name, descendant_types stores SUBTYPE values, not type values
        # A subtype S is a descendant of cls if cls.subtype appears in S's parent chain (MRO)
        descendant_types = [cls.subtype]  # Always include self
        for subtype_name, subtype_cls in cls.graph.__class__.subtypes.items():
            if subtype_name == cls.subtype:
                continue  # Skip self (already included)
            # Check if cls.subtype is in this subtype's ancestry
            for base in subtype_cls.__mro__[1:]:
                if hasattr(base, 'subtype') and base.subtype == cls.subtype:
                    descendant_types.append(subtype_name)
                    break

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
            VALUES (%(category)s, %(type)s, %(subtype)s, %(forward)s, %(back)s, %(parent_types)s, %(descendant_types)s)
            ON CONFLICT (category, type, subtype)
            DO UPDATE SET
                forward = EXCLUDED.forward,
                back = EXCLUDED.back,
                parent_types = EXCLUDED.parent_types,
                descendant_types = EXCLUDED.descendant_types
            """,
            category=cls.category,
            type=cls.type,
            subtype=cls.subtype,
            forward=forward_for_db if forward_for_db else None,
            back=list(cls._back_rels) if cls._back_rels else None,
            parent_types=parent_types if parent_types else [],
            descendant_types=descendant_types,
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
