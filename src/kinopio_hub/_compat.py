"""Async deadlines on supported Python versions."""
import sys

if sys.version_info >= (3, 11):
    from asyncio import timeout as timeout
else:
    from async_timeout import timeout as timeout
