CREATE SCHEMA IF NOT EXISTS {name};
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS {name}.object (
  id       BIGSERIAL PRIMARY KEY,
  category TEXT NOT NULL,
  type     TEXT NOT NULL,
  subtype  TEXT NOT NULL,
  attr     JSONB NOT NULL DEFAULT '{}'::JSONB,
  source   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_object_category ON {name}.object (category);
CREATE INDEX IF NOT EXISTS idx_object_type     ON {name}.object (type);
CREATE INDEX IF NOT EXISTS idx_object_subtype  ON {name}.object (subtype);
CREATE INDEX IF NOT EXISTS idx_object_attr_gin ON {name}.object USING gin (attr);

-- History table

CREATE TABLE IF NOT EXISTS {name}.history (
  id          BIGINT NOT NULL,
  validity    TSTZRANGE DEFAULT tstzrange(now(), 'infinity', '(]'),
  changed_by  TEXT DEFAULT current_user,
  category    TEXT NOT NULL,
  type        TEXT NOT NULL,
  subtype     TEXT NOT NULL,
  attr        JSONB NOT NULL,
  deleted     BOOLEAN NOT NULL DEFAULT false,
  source      TEXT DEFAULT NULL,
  PRIMARY KEY (id, validity WITHOUT OVERLAPS)
);

CREATE INDEX IF NOT EXISTS idx_history_attr_gin ON {name}.history USING gin (attr);
CREATE INDEX IF NOT EXISTS idx_history_deleted ON {name}.history (deleted);

-- Type metadata (relationships and inheritance)

CREATE TABLE IF NOT EXISTS {name}.meta (
  category TEXT NOT NULL,
  type TEXT NOT NULL,
  subtype TEXT NOT NULL,
  forward JSONB,
  back TEXT[],
  parent_types TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
  descendant_types TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
  PRIMARY KEY (category, type, subtype)
);

CREATE INDEX IF NOT EXISTS idx_meta_gin ON {name}.meta USING gin (forward);
CREATE INDEX IF NOT EXISTS idx_meta_descendants_gin ON {name}.meta USING gin (descendant_types);
