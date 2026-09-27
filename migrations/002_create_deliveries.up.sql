CREATE TABLE IF NOT EXISTS deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    driver_id UUID NOT NULL
        REFERENCES drivers(id),

    pickup_address TEXT NOT NULL,

    dropoff_address TEXT NOT NULL,

    status TEXT NOT NULL DEFAULT 'CREATED',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);