from typing import Type, Generic, Annotated, TypeVar, Any, get_origin, get_args, TYPE_CHECKING
from pydantic_core import core_schema

from .util import unwrap_optional

if TYPE_CHECKING:
    from .object import DBObject

T = TypeVar("T")


# Forward Links

class LinkInfo:
    __slots__ = ("target", "backlink")

    def __init__(self, target: Type, backlink: str | None = None):
        self.target = target
        self.backlink = backlink

class Link(Generic[T]):
    """
    Type annotation for foreign key relationships:
        post: Link[Author]                 # required link
        editor: Link[User] | None         # nullable link
        post: Link[Author, "posts"]       # with explicit backlink name
    """

    def __class_getitem__(cls, params):
        if isinstance(params, tuple):
            target, backlink = params[0], params[1] if len(params) > 1 else None
        else:
            target, backlink = params, None
        
        return Annotated[Any, LinkInfo(target, backlink)]


# Backward Links

class BacklinkInfo:
    """Metadata for reverse relationships."""
    __slots__ = ("target",)

    def __init__(self, target: Type['DBObject'] | str = 'DBObject'):
        # Store string types as-is for later resolution
        # They will be resolved when the relationship is processed during __init_subclass__
        self.target = target

class Backlink(Generic[T]):
    """Type annotation for reverse relationships."""

    def __class_getitem__(cls, target):
        return Annotated[Any, BacklinkInfo(target)]

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        return core_schema.any_schema()


def extract_link_info(type_: Type[Any]):
    type_ = unwrap_optional(type_)
    
    if isinstance(type_, type) and issubclass(type_, Backlink):
        return BacklinkInfo()

    meta = getattr(type_, "__metadata__", [])
    for m in meta:
        if isinstance(m, (LinkInfo, BacklinkInfo)):
            return m

    else:
        return None
