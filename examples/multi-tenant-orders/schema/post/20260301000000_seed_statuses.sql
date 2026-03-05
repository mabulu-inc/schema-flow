-- Verify the order_status enum was created correctly.
-- This is a no-op sanity check — the enum is declarative,
-- but post-scripts can be useful for seed data or validation.

DO $$
BEGIN
  PERFORM 'draft'::order_status;
  RAISE NOTICE 'order_status enum verified';
END $$;
