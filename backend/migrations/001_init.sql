-- ============================================
-- Схема базы данных маркетплейса
-- ============================================

-- Включаем расширение UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- TODO: Создать таблицу order_statuses
-- Столбцы: status (PK), description
CREATE TABLE order_statuses (
    status VARCHAR(20) PRIMARY KEY,
    description TEXT
);

-- TODO: Вставить значения статусов
-- created, paid, cancelled, shipped, completed
INSERT INTO order_statuses (status, description) VALUES
    ('created', 'Заказ создан'),
    ('paid', 'Заказ оплачен'),
    ('cancelled', 'Заказ отменён'),
    ('shipped', 'Заказ отправлен'),
    ('completed', 'Заказ завершён');

-- TODO: Создать таблицу users
-- Столбцы: id (UUID PK), email, name, created_at
-- Ограничения:
--   - email UNIQUE
--   - email NOT NULL и не пустой
--   - email валидный (regex через CHECK)
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT users_email_not_empty CHECK (email <> ''),
    CONSTRAINT users_email_format CHECK (
        email ~ '^[a-zA-Z0-9][a-zA-Z0-9._%-]*@[a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,}$'
    )
);

-- TODO: Создать таблицу orders
-- Столбцы: id (UUID PK), user_id (FK), status (FK), total_amount, created_at
-- Ограничения:
--   - user_id -> users(id)
--   - status -> order_statuses(status)
--   - total_amount >= 0
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'created',
    total_amount DECIMAL(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT orders_user_id_fk FOREIGN KEY (user_id) 
        REFERENCES users(id) ON DELETE RESTRICT,
    CONSTRAINT orders_status_fk FOREIGN KEY (status) 
        REFERENCES order_statuses(status),
    CONSTRAINT orders_total_amount_positive CHECK (total_amount >= 0)
);

-- TODO: Создать таблицу order_items
-- Столбцы: id (UUID PK), order_id (FK), product_name, price, quantity
-- Ограничения:
--   - order_id -> orders(id) CASCADE
--   - price >= 0
--   - quantity > 0
--   - product_name не пустой
CREATE TABLE order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    quantity INTEGER NOT NULL,
    
    CONSTRAINT order_items_order_id_fk FOREIGN KEY (order_id) 
        REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT order_items_price_positive CHECK (price >= 0),
    CONSTRAINT order_items_quantity_positive CHECK (quantity > 0),
    CONSTRAINT order_items_product_name_not_empty CHECK (product_name <> '')
);

-- TODO: Создать таблицу order_status_history
-- Столбцы: id (UUID PK), order_id (FK), status (FK), changed_at
-- Ограничения:
--   - order_id -> orders(id) CASCADE
--   - status -> order_statuses(status)
CREATE TABLE order_status_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT order_status_history_order_id_fk FOREIGN KEY (order_id) 
        REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT order_status_history_status_fk FOREIGN KEY (status) 
        REFERENCES order_statuses(status)
);

-- ============================================
-- КРИТИЧЕСКИЙ ИНВАРИАНТ: Нельзя оплатить заказ дважды
-- ============================================
-- TODO: Создать функцию триггера check_order_not_already_paid()
-- При изменении статуса на 'paid' проверить что его нет в истории
-- Если есть - RAISE EXCEPTION
CREATE OR REPLACE FUNCTION check_order_not_already_paid()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'paid' THEN
        IF EXISTS (
            SELECT 1 
            FROM order_status_history 
            WHERE order_id = NEW.id AND status = 'paid'
        ) THEN
            RAISE EXCEPTION 'Заказ % уже был оплачен', NEW.id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TODO: Создать триггер trigger_check_order_not_already_paid
-- BEFORE UPDATE ON orders FOR EACH ROW
CREATE TRIGGER trigger_check_order_not_already_paid
    BEFORE UPDATE OF status ON orders
    FOR EACH ROW
    EXECUTE FUNCTION check_order_not_already_paid();

-- ============================================
-- БОНУС (опционально)
-- ============================================
-- TODO: Триггер автоматического пересчета total_amount
CREATE OR REPLACE FUNCTION recalculate_order_total() RETURNS TRIGGER AS $$
BEGIN
    UPDATE orders
    SET total_amount = (
        SELECT COALESCE(SUM(price * quantity), 0)
        FROM order_items
        WHERE order_id = NEW.order_id
    )
    WHERE id = NEW.order_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_recalculate_order_total
    AFTER INSERT OR UPDATE OR DELETE ON order_items
    FOR EACH ROW
    EXECUTE FUNCTION recalculate_order_total();

-- TODO: Триггер автоматической записи в историю при изменении статуса
CREATE OR REPLACE FUNCTION log_order_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO order_status_history (order_id, status, changed_at)
        VALUES (NEW.id, NEW.status, CURRENT_TIMESTAMP);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_log_order_status_change
    AFTER UPDATE OF status ON orders
    FOR EACH ROW
    EXECUTE FUNCTION log_order_status_change();

-- TODO: Триггер записи начального статуса при создании заказа
CREATE OR REPLACE FUNCTION log_initial_order_status()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO order_status_history (order_id, status, changed_at)
    VALUES (NEW.id, NEW.status, NEW.created_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_log_initial_order_status
    AFTER INSERT ON orders
    FOR EACH ROW
    EXECUTE FUNCTION log_initial_order_status();