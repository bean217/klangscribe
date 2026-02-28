import traceback

def format_exception(e: Exception) -> str:
    """Format an exception with its traceback for better logging."""
    return f"{str(e)}\n{traceback.format_exc()}"