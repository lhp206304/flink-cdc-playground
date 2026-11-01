-- ============================================================
-- 设置 REPLICA IDENTITY FULL
-- 必须设置，否则 UPDATE/DELETE 操作拿不到变更前镜像（before）
-- ============================================================

-- 只对走 CDC 的事务表设置 REPLICA IDENTITY FULL
-- dim 维表不走 CDC，不需要设置
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;
ALTER TABLE public.payments REPLICA IDENTITY FULL;
