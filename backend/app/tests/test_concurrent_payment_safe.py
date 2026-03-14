"""
Тест для демонстрации РЕШЕНИЯ проблемы race condition.

Этот тест должен ПРОХОДИТЬ, подтверждая, что при использовании
pay_order_safe() заказ оплачивается только один раз.
"""

import asyncio
import pytest
import uuid
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.application.payment_service import PaymentService
from app.domain.exceptions import OrderAlreadyPaidError


# TODO: Настроить подключение к тестовой БД
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"


@pytest.fixture
async def db_session():
    """
    Создать сессию БД для тестов.
    
    TODO: Реализовать фикстуру (см. test_concurrent_payment_unsafe.py)
    """
    # TODO: Реализовать создание сессии
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
    )
    
    async_session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
    )
    
    async with async_session_maker() as session:
        yield session
        
    await engine.dispose()


@pytest.fixture
async def test_order(db_session):
    """
    Создать тестовый заказ со статусом 'created'.
    
    TODO: Реализовать фикстуру (см. test_concurrent_payment_unsafe.py)
    """
    # TODO: Реализовать создание тестового заказа
    user_id = uuid.uuid4()
    await db_session.execute(
        text("""
            INSERT INTO users (id, email, name, created_at)
            VALUES (:id, :email, :name, :created_at)
        """),
        {
            "id": user_id,
            "email": f"test_{uuid.uuid4()}@example.com",
            "name": "Test User",
            "created_at": datetime.now()
        }
    )
    
    order_id = uuid.uuid4()
    await db_session.execute(
        text("""
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, :status, :total_amount, :created_at)
        """),
        {
            "id": order_id,
            "user_id": user_id,
            "status": "created",
            "total_amount": 100.00,
            "created_at": datetime.now()
        }
    )
    
    await db_session.execute(
        text("""
            INSERT INTO order_status_history (id, order_id, status, changed_at)
            VALUES (gen_random_uuid(), :order_id, :status, :changed_at)
        """),
        {
            "order_id": order_id,
            "status": "created",
            "changed_at": datetime.now()
        }
    )
    await db_session.commit()
    
    yield order_id
    
    await db_session.execute(
        text("DELETE FROM order_status_history WHERE order_id = :order_id"),
        {"order_id": order_id}
    )
    await db_session.execute(
        text("DELETE FROM orders WHERE id = :order_id"),
        {"order_id": order_id}
    )
    await db_session.execute(
        text("DELETE FROM users WHERE id = :user_id"),
        {"user_id": user_id}
    )
    await db_session.commit()


@pytest.mark.asyncio
async def test_concurrent_payment_safe_prevents_race_condition(db_session, test_order):
    """
    Тест демонстрирует решение проблемы race condition с помощью pay_order_safe().
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ: Тест ПРОХОДИТ, подтверждая, что заказ был оплачен только один раз.
    Это показывает, что метод pay_order_safe() защищен от конкурентных запросов.
    
    TODO: Реализовать тест следующим образом:
    
    1. Создать два экземпляра PaymentService с РАЗНЫМИ сессиями
        (это имитирует два независимых HTTP-запроса)
       
    2. Запустить два параллельных вызова pay_order_safe():
       
        async def payment_attempt_1():
            service1 = PaymentService(session1)
            return await service1.pay_order_safe(order_id)
            
        async def payment_attempt_2():
            service2 = PaymentService(session2)
            return await service2.pay_order_safe(order_id)
            
        results = await asyncio.gather(
            payment_attempt_1(),
            payment_attempt_2(),
            return_exceptions=True
        )
       
    3. Проверить результаты:
        - Одна попытка должна УСПЕШНО завершиться
        - Вторая попытка должна выбросить OrderAlreadyPaidError ИЛИ вернуть ошибку
        
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        error_count = sum(1 for r in results if isinstance(r, Exception))
        
        assert success_count == 1, "Ожидалась одна успешная оплата"
        assert error_count == 1, "Ожидалась одна неудачная попытка"
       
    4. Проверить историю оплат:
       
        service = PaymentService(session)
        history = await service.get_payment_history(order_id)
        
        # ОЖИДАЕМ ОДНУ ЗАПИСЬ 'paid' - проблема решена!
        assert len(history) == 1, "Ожидалась 1 запись об оплате (БЕЗ RACE CONDITION!)"
       
    5. Вывести информацию об успешном решении:
       
        print(f"✅ RACE CONDITION PREVENTED!")
        print(f"Order {order_id} was paid only ONCE:")
        print(f"  - {history[0]['changed_at']}: status = {history[0]['status']}")
        print(f"Second attempt was rejected: {results[1]}")
    """
    # TODO: Реализовать тест, демонстрирующий решение race condition
    order_id = test_order
    engine = create_async_engine(DATABASE_URL, echo=False)
    session1 = AsyncSession(engine)
    session2 = AsyncSession(engine)
    
    async def payment_attempt_1():
        service1 = PaymentService(session1)
        return await service1.pay_order_safe(order_id)
        
    async def payment_attempt_2():
        service2 = PaymentService(session2)
        return await service2.pay_order_safe(order_id)
        
    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True
    )
    
    success_count = sum(1 for r in results if not isinstance(r, Exception))
    error_count = sum(1 for r in results if isinstance(r, Exception))
    
    assert success_count == 1, f"Ожидалась одна успешная оплата"
    assert error_count == 1, f"Ожидалась одна неудачная попытка"
    
    for r in results:
        if isinstance(r, Exception):
            assert isinstance(r, OrderAlreadyPaidError), f"Ожидалась OrderAlreadyPaidError, получена {type(r)}"
    

    service = PaymentService(db_session)
    history = await service.get_payment_history(order_id)
    
    # ОЖИДАЕМ ОДНУ ЗАПИСЬ 'paid' - проблема решена!
    assert len(history) == 1, f"Ожидалась 1 запись об оплате (БЕЗ RACE CONDITION!)"
    
    print(f"\n✅ RACE CONDITION PREVENTED!")
    print(f"Order {order_id} was paid only ONCE:")
    print(f"  - {history[0]['changed_at']}: status = {history[0]['status']}")
    print(f"Second attempt was rejected: {results[1]}")
    
    await session1.close()
    await session2.close()
    await engine.dispose()

'''
@pytest.mark.asyncio
async def test_concurrent_payment_safe_with_explicit_timing():
    """
    Дополнительный тест: проверить работу блокировок с явной задержкой.
    
    TODO: Реализовать тест с добавлением задержки в первой транзакции:
    
    1. Первая транзакция:
       - Начать транзакцию
       - Заблокировать заказ (FOR UPDATE)
       - Добавить задержку (asyncio.sleep(1))
       - Оплатить
       - Commit
       
    2. Вторая транзакция (запустить через 0.1 секунды после первой):
       - Начать транзакцию
       - Попытаться заблокировать заказ (FOR UPDATE)
       - ДОЛЖНА ЖДАТЬ освобождения блокировки от первой транзакции
       - После освобождения - увидеть обновленный статус 'paid'
       - Выбросить OrderAlreadyPaidError
       
    3. Проверить временные метки:
       - Вторая транзакция должна завершиться ПОЗЖЕ первой
       - Разница должна быть >= 1 секунды (время задержки)
       
    Это подтверждает, что FOR UPDATE действительно блокирует строку.
    """
    # TODO: Реализовать тест с проверкой блокировки
    order_id = test_order
    engine = create_async_engine(DATABASE_URL, echo=False)
    session1 = AsyncSession(engine)
    session2 = AsyncSession(engine)
    
    async def payment_attempt_with_delay():
        service1 = PaymentService(session1)
        start_time = datetime.now()
        await session1.execute(text("BEGIN"))
        
        try:
            select_query = text("""
                SELECT status FROM orders 
                WHERE id = :order_id 
                FOR UPDATE
            """)
            await session1.execute(select_query, {"order_id": order_id})
            await asyncio.sleep(1)
            result = await service1.pay_order_safe(order_id)
            await session1.commit()
            end_time = datetime.now()
            return {"result": result, "start": start_time, "end": end_time}
        except Exception as e:
            await session1.rollback()
            return {"error": e, "start": start_time, "end": datetime.now()}
    
    async def payment_attempt_with_blocking():
        service2 = PaymentService(session2)
        start_time = datetime.now()
        await asyncio.sleep(0.1)
        
        try:
            result = await service2.pay_order_safe(order_id)
            await session2.commit()
            end_time = datetime.now()
            return {"result": result, "start": start_time, "end": end_time}
        except Exception as e:
            await session2.rollback()
            end_time = datetime.now()
            return {"error": e, "start": start_time, "end": end_time}
    
    results = await asyncio.gather(
        payment_attempt_with_delay(),
        payment_attempt_with_blocking()
    )
    
    assert "error" not in results[0], f"Первая транзакция не должна была упасть: {results[0].get('error')}"
    assert results[0]["result"]["status"] == "paid"
    
    assert "error" in results[1], "Вторая транзакция должна была упасть"
    assert isinstance(results[1]["error"], OrderAlreadyPaidError)
    
    assert results[1]["end"] > results[0]["end"], "Вторая транзакция должна завершиться позже первой"
    
    time_diff = (results[1]["end"] - results[0]["end"]).total_seconds()
    assert time_diff >= 0.99, f"Ожидалась задержка >= 1 сек, получено {time_diff} сек"
    
    await session1.close()
    await session2.close()
    await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_payment_safe_multiple_orders():
    """
    Дополнительный тест: проверить, что блокировки не мешают разным заказам.
    
    TODO: Реализовать тест:
    1. Создать ДВА разных заказа
    2. Оплатить их ПАРАЛЛЕЛЬНО с помощью pay_order_safe()
    3. Проверить, что ОБА успешно оплачены
    
    Это показывает, что FOR UPDATE блокирует только конкретную строку,
    а не всю таблицу, что важно для производительности.
    """
    # TODO: Реализовать тест с несколькими заказами
    raise NotImplementedError("TODO: Реализовать test_concurrent_payment_safe_multiple_orders")
'''

if __name__ == "__main__":
    """
    Запуск теста:
    
    cd backend
    export PYTHONPATH=$(pwd)
    pytest app/tests/test_concurrent_payment_safe.py -v -s
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    ✅ test_concurrent_payment_safe_prevents_race_condition PASSED
    
    Вывод должен показывать:
    ✅ RACE CONDITION PREVENTED!
    Order XXX was paid only ONCE:
      - 2024-XX-XX: status = paid
    Second attempt was rejected: OrderAlreadyPaidError(...)
    """
    pytest.main([__file__, "-v", "-s"])
