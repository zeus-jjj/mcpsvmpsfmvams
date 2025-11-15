-- Миграция для добавления функционала паузы уведомлений в систему
-- Выполнить в PostgreSQL базе данных

-- =====================================================
-- 1. Добавляем новые поля в таблицу notifications
-- =====================================================
ALTER TABLE notifications
ADD COLUMN IF NOT EXISTS paused_at TIMESTAMP DEFAULT NULL,
ADD COLUMN IF NOT EXISTS pause_reason VARCHAR(50) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();

COMMENT ON COLUMN notifications.paused_at IS 'Время когда уведомление было поставлено на паузу';
COMMENT ON COLUMN notifications.pause_reason IS 'Причина паузы (inactivity, user_request, etc)';
COMMENT ON COLUMN notifications.created_at IS 'Время создания уведомления';

-- =====================================================
-- 2. Добавляем поле last_activity в таблицу users
-- =====================================================
ALTER TABLE users
ADD COLUMN IF NOT EXISTS last_activity TIMESTAMP DEFAULT NOW();

COMMENT ON COLUMN users.last_activity IS 'Время последней активности пользователя';

-- Обновляем last_activity для существующих пользователей
UPDATE users
SET last_activity = COALESCE(
    (SELECT MAX(timestamp) FROM user_history WHERE user_id = users.id),
    users.timestamp_registration,
    NOW()
)
WHERE last_activity IS NULL;

-- =====================================================
-- 3. Создаём индексы для оптимизации запросов
-- =====================================================

-- Индекс для быстрого поиска приостановленных уведомлений
CREATE INDEX IF NOT EXISTS idx_notifications_paused
ON notifications(user_id, is_active, pause_reason)
WHERE pause_reason IS NOT NULL;

-- Индекс для активных уведомлений по времени
CREATE INDEX IF NOT EXISTS idx_notifications_active_time
ON notifications(is_active, time_to_send)
WHERE is_active = true;

-- Индекс для поиска неактивных пользователей
CREATE INDEX IF NOT EXISTS idx_users_last_activity
ON users(last_activity);

-- =====================================================
-- 4. Создаём представление для мониторинга неактивных
-- =====================================================
CREATE OR REPLACE VIEW inactive_users_monitor AS
SELECT
    u.id as user_id,
    u.username,
    u.first_name,
    u.last_name,
    u.timestamp_registration,
    u.last_activity,
    EXTRACT(DAY FROM NOW() - u.last_activity) as days_inactive,
    (SELECT COUNT(*) FROM notifications
     WHERE user_id = u.id AND is_active = true) as active_notifications,
    (SELECT COUNT(*) FROM notifications
     WHERE user_id = u.id AND pause_reason = 'inactivity') as paused_notifications
FROM users u
WHERE u.last_activity < NOW() - INTERVAL '45 days'
  AND EXISTS (
    SELECT 1 FROM notifications n
    WHERE n.user_id = u.id AND n.is_active = true
  )
ORDER BY days_inactive DESC;

COMMENT ON VIEW inactive_users_monitor IS 'Мониторинг неактивных пользователей для управления уведомлениями';

-- =====================================================
-- 5. Функция для массовой паузы неактивных
-- =====================================================
CREATE OR REPLACE FUNCTION pause_inactive_users_batch()
RETURNS TABLE(
    user_id INTEGER,
    username VARCHAR,
    days_inactive INTEGER,
    notifications_paused INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    paused_count INTEGER;
BEGIN
    FOR rec IN
        SELECT
            u.id,
            u.username,
            EXTRACT(DAY FROM NOW() - u.last_activity)::INTEGER as days
        FROM users u
        WHERE u.last_activity < NOW() - INTERVAL '45 days'
        AND EXISTS (
            SELECT 1 FROM notifications n
            WHERE n.user_id = u.id
            AND n.is_active = true
            AND n.pause_reason IS NULL
        )
    LOOP
        -- Ставим на паузу уведомления
        UPDATE notifications
        SET is_active = false,
            paused_at = NOW(),
            pause_reason = 'inactivity'
        WHERE notifications.user_id = rec.id
        AND is_active = true
        AND pause_reason IS NULL;

        -- Получаем количество приостановленных
        GET DIAGNOSTICS paused_count = ROW_COUNT;

        IF paused_count > 0 THEN
            -- Логируем в историю
            INSERT INTO user_history (user_id, text)
            VALUES (rec.id, 'Уведомления приостановлены из-за неактивности (' || rec.days || ' дней)');

            -- Возвращаем результат
            user_id := rec.id;
            username := rec.username;
            days_inactive := rec.days;
            notifications_paused := paused_count;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION pause_inactive_users_batch() IS 'Массовая приостановка уведомлений для неактивных пользователей';

-- =====================================================
-- 6. Проверка статистики после миграции
-- =====================================================
DO $$
BEGIN
    RAISE NOTICE '===== Статистика после миграции =====';
    RAISE NOTICE 'Всего пользователей: %', (SELECT COUNT(*) FROM users);
    RAISE NOTICE 'Активных уведомлений: %', (SELECT COUNT(*) FROM notifications WHERE is_active = true);
    RAISE NOTICE 'Неактивных пользователей (>45 дней): %', (SELECT COUNT(*) FROM users WHERE last_activity < NOW() - INTERVAL '45 days');
    RAISE NOTICE 'Пользователей с активными уведомлениями: %', (SELECT COUNT(DISTINCT user_id) FROM notifications WHERE is_active = true);
    RAISE NOTICE '======================================';
END $$;

-- =====================================================
-- 7. Примеры использования
-- =====================================================

-- Просмотр неактивных пользователей:
-- SELECT * FROM inactive_users_monitor;

-- Приостановка уведомлений для неактивных:
-- SELECT * FROM pause_inactive_users_batch();

-- Проверка приостановленных уведомлений конкретного пользователя:
-- SELECT * FROM notifications
-- WHERE user_id = ?
-- AND pause_reason = 'inactivity'
-- ORDER BY paused_at DESC;

-- Возобновление уведомлений для пользователя:
-- UPDATE notifications
-- SET is_active = true,
--     paused_at = NULL,
--     pause_reason = NULL,
--     time_to_send = EXTRACT(EPOCH FROM NOW() + INTERVAL '5 minutes')::INTEGER
-- WHERE user_id = ?
-- AND pause_reason = 'inactivity';
