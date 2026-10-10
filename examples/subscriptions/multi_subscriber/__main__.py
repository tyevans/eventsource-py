"""
Executable entry point for multi-subscriber example.
"""

from __future__ import annotations

import asyncio

from examples.subscriptions.multi_subscriber.runner import main

if __name__ == "__main__":
    asyncio.run(main())
