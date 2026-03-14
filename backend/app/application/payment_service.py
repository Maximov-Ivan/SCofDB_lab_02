"""Сервис для демонстрации конкурентных оплат.

Этот модуль содержит два метода оплаты:
1. pay_order_unsafe() - небезопасная реализация (READ COMMITTED без блокировок)
2. pay_order_safe() - безопасная реализация (REPEATABLE READ + FOR UPDATE)
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.exceptions import OrderAlreadyPaidError, OrderNotFoundError


class PaymentService:
    """Сервис для обработки платежей с разными уровнями изоляции."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def pay_order_unsafe(self, order_id: uuid.UUID) -> dict:
        """
        НЕБЕЗОПАСНАЯ реализация оплаты заказа.
        
        Использует READ COMMITTED (по умолчанию) без блокировок.
        ЛОМАЕТСЯ при конкурентных запросах - может привести к двойной оплате!
        
        TODO: Реализовать метод следующим образом:
        
        1. Прочитать текущий статус заказа:
           SELECT status FROM orders WHERE id = :order_id
           
        2. Проверить, что статус = 'created'
           Если нет - выбросить OrderAlreadyPaidError
           
        3. Изменить статус на 'paid':
           UPDATE orders SET status = 'paid' 
           WHERE id = :order_id AND status = 'created'
           
        4. Записать изменение в историю:
           INSERT INTO order_status_history (id, order_id, status, changed_at)
           VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
           
        5. Сделать commit
        
        ВАЖНО: НЕ используйте FOR UPDATE!
        ВАЖНО: НЕ меняйте уровень изоляции (оставьте READ COMMITTED по умолчанию)!
        
        Args:
            order_id: ID заказа для оплаты
            
        Returns:
            dict с информацией о заказе после оплаты
            
        Raises:
            OrderNotFoundError: если заказ не найден
            OrderAlreadyPaidError: если заказ уже оплачен
        """
        # TODO: Реализовать логику оплаты БЕЗ блокировок
        async with self.session.begin():
            select_query = text("""
                SELECT status, user_id, total_amount, created_at
                FROM orders
                WHERE id = :order_id
            """)
            result = await self.session.execute(select_query, {"order_id": order_id})
            order_row = result.first()
            if not order_row:
                raise OrderNotFoundError(order_id)
            status, user_id, total_amount, created_at = order_row
            
            if status != 'created':
                raise OrderAlreadyPaidError(order_id)
            
            update_query = text("""
                UPDATE orders 
                SET status = 'paid' 
                WHERE id = :order_id AND status = 'created'
            """)
            await self.session.execute(update_query, {"order_id": order_id})
            
            history_query = text("""
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
            """)
            await self.session.execute(history_query, {"order_id": order_id})

        return {
            "id": order_id,
            "user_id": user_id,
            "status": "paid",
            "total_amount": total_amount,
            "created_at": created_at,
            "paid_at": datetime.now()
        }

    async def pay_order_safe(self, order_id: uuid.UUID) -> dict:
        """
        БЕЗОПАСНАЯ реализация оплаты заказа.
        
        Использует REPEATABLE READ + FOR UPDATE для предотвращения race condition.
        Корректно работает при конкурентных запросах.
        
        TODO: Реализовать метод следующим образом:
        
        1. Установить уровень изоляции REPEATABLE READ:
           await self.session.execute(
               text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
           )
           
        2. Заблокировать строку заказа для обновления:
           SELECT status FROM orders WHERE id = :order_id FOR UPDATE
           
           ВАЖНО: FOR UPDATE гарантирует, что другие транзакции будут ЖДАТЬ
           освобождения блокировки. Это предотвращает race condition.
           
        3. Проверить, что статус = 'created'
           Если нет - выбросить OrderAlreadyPaidError
           
        4. Изменить статус на 'paid':
           UPDATE orders SET status = 'paid' 
           WHERE id = :order_id AND status = 'created'
           
        5. Записать изменение в историю:
           INSERT INTO order_status_history (id, order_id, status, changed_at)
           VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
           
        6. Сделать commit
        
        ВАЖНО: Обязательно используйте FOR UPDATE!
        ВАЖНО: Обязательно установите REPEATABLE READ!
        
        Args:
            order_id: ID заказа для оплаты
            
        Returns:
            dict с информацией о заказе после оплаты
            
        Raises:
            OrderNotFoundError: если заказ не найден
            OrderAlreadyPaidError: если заказ уже оплачен
        """
        # TODO: Реализовать логику оплаты С блокировками
        async with self.session.begin():
            await self.session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            )
            
            select_query = text("""
                SELECT status, user_id, total_amount, created_at
                FROM orders
                WHERE id = :order_id
                FOR UPDATE
            """)
            result = await self.session.execute(select_query, {"order_id": order_id})
            order_row = result.first()
            if not order_row:
                raise OrderNotFoundError(order_id)
            
            if order_row[0] != 'created':
                raise OrderAlreadyPaidError(order_id)
            
            status, user_id, total_amount, created_at = order_row
            update_query = text("""
                UPDATE orders SET status = 'paid' 
                WHERE id = :order_id AND status = 'created'
            """)
            await self.session.execute(update_query, {"order_id": order_id})
            
            history_query = text("""
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
            """)
            await self.session.execute(history_query, {"order_id": order_id})
        
        return {
            "id": order_id,
            "user_id": user_id,
            "status": "paid",
            "total_amount": total_amount,
            "created_at": created_at,
            "paid_at": datetime.now()
        }

    async def get_payment_history(self, order_id: uuid.UUID) -> list[dict]:
        """
        Получить историю оплат для заказа.
        
        Используется для проверки, сколько раз был оплачен заказ.
        
        TODO: Реализовать метод:
        
        SELECT id, order_id, status, changed_at
        FROM order_status_history
        WHERE order_id = :order_id AND status = 'paid'
        ORDER BY changed_at
        
        Args:
            order_id: ID заказа
            
        Returns:
            Список словарей с записями об оплате
        """
        # TODO: Реализовать получение истории оплат
        async with self.session.begin():
            query = text("""
                SELECT id, order_id, status, changed_at
                FROM order_status_history
                WHERE order_id = :order_id AND status = 'paid'
                ORDER BY changed_at
            """)
            result = await self.session.execute(query, {"order_id": order_id})
            
            payments = []
            for row in result:
                payments.append({
                    "id": row[0],
                    "order_id": row[1],
                    "status": row[2],
                    "changed_at": row[3]
                })
        return payments
