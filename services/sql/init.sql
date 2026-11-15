CREATE TABLE IF NOT EXISTS scores (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL,
    us_state VARCHAR(2) NOT NULL,
    merch TEXT NOT NULL,
    cat_id TEXT NOT NULL,
    transaction_amount NUMERIC(14, 2) NOT NULL,
    is_online BOOLEAN NOT NULL DEFAULT FALSE,
    is_international BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    transaction_time TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_scores_created_at ON scores (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_scores_model_version ON scores (model_version);
CREATE INDEX IF NOT EXISTS idx_scores_state_merch ON scores (us_state, merch);
CREATE INDEX IF NOT EXISTS idx_scores_cat_created ON scores (cat_id, created_at DESC);
