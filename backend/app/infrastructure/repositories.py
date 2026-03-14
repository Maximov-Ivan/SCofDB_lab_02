"""Реализация репозиториев с использованием SQLAlchemy."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User
from app.domain.order import Order, OrderItem, OrderStatus, OrderStatusChange


class UserRepository:
    """Репозиторий для User."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(user: User) -> None
    # Используйте INSERT ... ON CONFLICT DO UPDATE
    async def save(self, user: User) -> None:
        query = text("""
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, :created_at)
            ON CONFLICT (id) DO UPDATE SET
                email = EXCLUDED.email,
                name = EXCLUDED.name
        """)
        await self.session.execute(
            query,
            {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "created_at": user.created_at
            }
        )

    # TODO: Реализовать find_by_id(user_id: UUID) -> Optional[User]
    async def find_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        query = text("""
            SELECT id, email, name, created_at
            FROM users
            WHERE id = :id
        """)
        result = await self.session.execute(query, {"id": user_id})
        row = result.first()
        if not row:
            return None
        
        user = object.__new__(User)
        user.id = row[0]
        user.email = row[1]
        user.name = row[2]
        user.created_at = row[3]
        return user

    # TODO: Реализовать find_by_email(email: str) -> Optional[User]
    async def find_by_email(self, email: str) -> Optional[User]:
        query = text("""
            SELECT id, email, name, created_at
            FROM users
            WHERE email = :email
        """)
        result = await self.session.execute(query, {"email": email})
        row = result.first()
        if not row:
            return None
        
        user = object.__new__(User)
        user.id = row[0]
        user.email = row[1]
        user.name = row[2]
        user.created_at = row[3]
        return user

    # TODO: Реализовать find_all() -> List[User]
    async def find_all(self) -> List[User]:
        query = text("""
            SELECT id, email, name, created_at
            FROM users
            ORDER BY created_at DESC
        """)
        result = await self.session.execute(query)

        users = []
        for row in result:
            user = object.__new__(User)
            user.id = row[0]
            user.email = row[1]
            user.name = row[2]
            user.created_at = row[3]
            users.append(user)
        return users


class OrderRepository:
    """Репозиторий для Order."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # TODO: Реализовать save(order: Order) -> None
    # Сохранить заказ, товары и историю статусов
    async def save(self, order: Order) -> None:
        order_query = text("""
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, :status, :total_amount, :created_at)
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                status = EXCLUDED.status,
                total_amount = EXCLUDED.total_amount
        """)
        await self.session.execute(
            order_query,
            {
                "id": order.id,
                "user_id": order.user_id,
                "status": order.status.value,
                "total_amount": order.total_amount,
                "created_at": order.created_at
            }
        )
        
        if order.items:
            await self.session.execute(
                text("DELETE FROM order_items WHERE order_id = :order_id"),
                {"order_id": order.id}
            )
            for item in order.items:
                await self.session.execute(
                    text("""
                        INSERT INTO order_items (id, order_id, product_name, price, quantity)
                        VALUES (:id, :order_id, :product_name, :price, :quantity)
                    """),
                    {
                        "id": item.id,
                        "order_id": order.id,
                        "product_name": item.product_name,
                        "price": item.price,
                        "quantity": item.quantity
                    }
                )
        
        if order.status_history:
            await self.session.execute(
                text("DELETE FROM order_status_history WHERE order_id = :order_id"),
                {"order_id": order.id}
            )
            for history in order.status_history:
                await self.session.execute(
                    text("""
                        INSERT INTO order_status_history (id, order_id, status, changed_at)
                        VALUES (:id, :order_id, :status, :changed_at)
                    """),
                    {
                        "id": history.id,
                        "order_id": order.id,
                        "status": history.status.value,
                        "changed_at": history.changed_at
                    }
                )

    # TODO: Реализовать find_by_id(order_id: UUID) -> Optional[Order]
    # Загрузить заказ со всеми товарами и историей
    # Используйте object.__new__(Order) чтобы избежать __post_init__
    async def find_by_id(self, order_id: uuid.UUID) -> Optional[Order]:
        order_query = text("""
            SELECT id, user_id, status, total_amount, created_at
            FROM orders
            WHERE id = :id
        """)
        result = await self.session.execute(order_query, {"id": order_id})
        order_row = result.first()
        if not order_row:
            return None
        
        order = object.__new__(Order)
        order.id = order_row[0]
        order.user_id = order_row[1]
        order.status = OrderStatus(order_row[2])
        order.total_amount = order_row[3]
        order.created_at = order_row[4]
        order.items = []
        order.status_history = []
        
        items_query = text("""
            SELECT id, order_id, product_name, price, quantity
            FROM order_items
            WHERE order_id = :order_id
            ORDER BY product_name
        """)
        items_result = await self.session.execute(items_query, {"order_id": order_id})
        
        for row in items_result:
            item = object.__new__(OrderItem)
            item.id = row[0]
            item.order_id = row[1]
            item.product_name = row[2]
            item.price = row[3]
            item.quantity = row[4]
            order.items.append(item)
        
        history_query = text("""
            SELECT id, order_id, status, changed_at
            FROM order_status_history
            WHERE order_id = :order_id
            ORDER BY changed_at
        """)
        history_result = await self.session.execute(history_query, {"order_id": order_id})
        
        for row in history_result:
            history = object.__new__(OrderStatusChange)
            history.id = row[0]
            history.order_id = row[1]
            history.status = OrderStatus(row[2])
            history.changed_at = row[3]
            order.status_history.append(history)
        
        return order

    # TODO: Реализовать find_by_user(user_id: UUID) -> List[Order]
    async def find_by_user(self, user_id: uuid.UUID) -> List[Order]:
        query = text("""
            SELECT id
            FROM orders
            WHERE user_id = :user_id
            ORDER BY created_at DESC
        """)
        result = await self.session.execute(query, {"user_id": user_id})

        orders = []
        for row in result:
            order = await self.find_by_id(row[0])
            if order:
                orders.append(order)
        return orders

    # TODO: Реализовать find_all() -> List[Order]
    async def find_all(self) -> List[Order]:
        query = text("""
            SELECT id
            FROM orders
            ORDER BY created_at DESC
        """)
        result = await self.session.execute(query)

        orders = []
        for row in result:
            order = await self.find_by_id(row[0])
            if order:
                orders.append(order)
        return orders
