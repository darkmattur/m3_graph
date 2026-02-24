from typing import Type, Any, get_origin, get_args

# Typing utilities
def is_optional(type_: Any) -> bool:
    """Check if a type annotation allows None."""
    origin = get_origin(type_)
    return origin is not None and type(None) in get_args(type_)

def unwrap_optional(tp: Any) -> Any:
    """Extract the non-None type from Optional[T]."""
    if not is_optional(tp):
        return tp
    return next(t for t in get_args(tp) if t is not type(None))
