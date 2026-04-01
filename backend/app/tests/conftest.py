"""Pytest configuration and fixtures."""

import os
# Set test database URL BEFORE any app imports
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"

import asyncio
import pytest
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text

from app.infrastructure.db import get_db
