CREATE SCHEMA IF NOT EXISTS commerce;

CREATE TABLE IF NOT EXISTS commerce.customers (
    customer_id BIGINT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    customer_status TEXT NOT NULL DEFAULT 'active',
    city TEXT NOT NULL,
    state_code CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT customers_status_check
        CHECK (customer_status IN ('active', 'inactive', 'suspended'))
);

CREATE TABLE IF NOT EXISTS commerce.products (
    product_id BIGINT PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT products_price_check
        CHECK (unit_price > 0)
);

CREATE TABLE IF NOT EXISTS commerce.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_status TEXT NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    order_total NUMERIC(14, 2) NOT NULL,
    ordered_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT orders_customer_fk
        FOREIGN KEY (customer_id)
        REFERENCES commerce.customers (customer_id),
    CONSTRAINT orders_status_check
        CHECK (
            order_status IN (
                'created',
                'confirmed',
                'processing',
                'shipped',
                'delivered',
                'cancelled'
            )
        ),
    CONSTRAINT orders_total_check
        CHECK (order_total >= 0),
    CONSTRAINT orders_currency_check
        CHECK (currency = 'USD')
);

CREATE TABLE IF NOT EXISTS commerce.order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    line_total NUMERIC(14, 2)
        GENERATED ALWAYS AS (quantity * unit_price) STORED,
    CONSTRAINT order_items_order_fk
        FOREIGN KEY (order_id)
        REFERENCES commerce.orders (order_id),
    CONSTRAINT order_items_product_fk
        FOREIGN KEY (product_id)
        REFERENCES commerce.products (product_id),
    CONSTRAINT order_items_quantity_check
        CHECK (quantity > 0),
    CONSTRAINT order_items_price_check
        CHECK (unit_price > 0),
    CONSTRAINT order_items_unique_product
        UNIQUE (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS commerce.payments (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    payment_status TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT payments_order_fk
        FOREIGN KEY (order_id)
        REFERENCES commerce.orders (order_id),
    CONSTRAINT payments_status_check
        CHECK (
            payment_status IN (
                'pending',
                'authorized',
                'captured',
                'failed',
                'refunded'
            )
        ),
    CONSTRAINT payments_method_check
        CHECK (
            payment_method IN (
                'credit_card',
                'debit_card',
                'digital_wallet',
                'bank_transfer'
            )
        ),
    CONSTRAINT payments_amount_check
        CHECK (amount >= 0)
);

CREATE TABLE IF NOT EXISTS commerce.shipments (
    shipment_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    shipment_status TEXT NOT NULL,
    carrier TEXT,
    tracking_code TEXT UNIQUE,
    shipped_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT shipments_order_fk
        FOREIGN KEY (order_id)
        REFERENCES commerce.orders (order_id),
    CONSTRAINT shipments_status_check
        CHECK (
            shipment_status IN (
                'pending',
                'packed',
                'shipped',
                'in_transit',
                'delivered',
                'returned'
            )
        ),
    CONSTRAINT shipments_date_check
        CHECK (
            delivered_at IS NULL
            OR shipped_at IS NULL
            OR delivered_at >= shipped_at
        )
);

CREATE INDEX IF NOT EXISTS orders_customer_index
    ON commerce.orders (customer_id);

CREATE INDEX IF NOT EXISTS orders_ordered_at_index
    ON commerce.orders (ordered_at);

CREATE INDEX IF NOT EXISTS order_items_order_index
    ON commerce.order_items (order_id);

CREATE INDEX IF NOT EXISTS payments_order_index
    ON commerce.payments (order_id);

CREATE INDEX IF NOT EXISTS shipments_order_index
    ON commerce.shipments (order_id);

CREATE OR REPLACE FUNCTION commerce.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER customers_set_updated_at
BEFORE UPDATE ON commerce.customers
FOR EACH ROW
EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER products_set_updated_at
BEFORE UPDATE ON commerce.products
FOR EACH ROW
EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER orders_set_updated_at
BEFORE UPDATE ON commerce.orders
FOR EACH ROW
EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER payments_set_updated_at
BEFORE UPDATE ON commerce.payments
FOR EACH ROW
EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER shipments_set_updated_at
BEFORE UPDATE ON commerce.shipments
FOR EACH ROW
EXECUTE FUNCTION commerce.set_updated_at();