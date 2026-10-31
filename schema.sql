CREATE TABLE IF NOT EXISTS brute_checkpoints (
    provider TEXT NOT NULL,
    min_length INTEGER NOT NULL,
    max_length INTEGER NOT NULL,
    alphabet TEXT NOT NULL,
    next_index INTEGER NOT NULL,
    total INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, min_length, max_length, alphabet)
);

CREATE TABLE IF NOT EXISTS job_update_checkpoints (
    provider TEXT NOT NULL,
    company_slug TEXT NOT NULL,
    completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, company_slug)
);

CREATE TABLE IF NOT EXISTS provider_companies (
    provider TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (provider, name)
);

CREATE TABLE IF NOT EXISTS indexed_jobs (
    provider TEXT NOT NULL,
    company_slug TEXT NOT NULL,
    job_id TEXT NOT NULL,
    title TEXT NOT NULL,
    company_name TEXT NOT NULL,
    location_name TEXT NOT NULL,
    location_json TEXT NOT NULL DEFAULT '',
    details_url TEXT NOT NULL DEFAULT '',
    apply_url TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    first_published TEXT NOT NULL DEFAULT '',
    employment_type TEXT NOT NULL DEFAULT '',
    workplace_type TEXT NOT NULL DEFAULT '',
    department TEXT NOT NULL DEFAULT '',
    education TEXT NOT NULL DEFAULT '',
    experience TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    attributes_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (provider, company_slug, job_id)
);

CREATE INDEX IF NOT EXISTS indexed_jobs_title_idx
    ON indexed_jobs(title);
CREATE INDEX IF NOT EXISTS indexed_jobs_company_idx
    ON indexed_jobs(company_name);
CREATE INDEX IF NOT EXISTS indexed_jobs_location_idx
    ON indexed_jobs(location_name);
CREATE INDEX IF NOT EXISTS indexed_jobs_date_idx
    ON indexed_jobs(first_published);
CREATE INDEX IF NOT EXISTS indexed_jobs_employment_idx
    ON indexed_jobs(employment_type);
CREATE INDEX IF NOT EXISTS indexed_jobs_workplace_idx
    ON indexed_jobs(workplace_type);

CREATE TABLE IF NOT EXISTS indexed_bookmarks (
    provider TEXT NOT NULL,
    company_slug TEXT NOT NULL,
    job_id TEXT NOT NULL,
    title TEXT NOT NULL,
    company_name TEXT NOT NULL,
    location_name TEXT NOT NULL,
    location_json TEXT NOT NULL DEFAULT '',
    details_url TEXT NOT NULL DEFAULT '',
    apply_url TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    first_published TEXT NOT NULL DEFAULT '',
    employment_type TEXT NOT NULL DEFAULT '',
    workplace_type TEXT NOT NULL DEFAULT '',
    department TEXT NOT NULL DEFAULT '',
    education TEXT NOT NULL DEFAULT '',
    experience TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    attributes_json TEXT NOT NULL DEFAULT '{}',
    is_available INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, company_slug, job_id)
);
