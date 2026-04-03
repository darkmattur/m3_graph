-- Helper: append an id to a jsonb array if not already present
CREATE OR REPLACE FUNCTION {name}.jsonb_ids_add_unique(arr jsonb, v bigint)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN arr IS NULL OR jsonb_typeof(arr) <> 'array' THEN to_jsonb(ARRAY[v])
    WHEN arr @> to_jsonb(v)                          THEN arr
    ELSE arr || to_jsonb(v)
  END;
$$;

-- Helper: remove an id from a jsonb array
CREATE OR REPLACE FUNCTION {name}.jsonb_ids_remove(arr jsonb, v bigint)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    (SELECT to_jsonb(array_agg(x::bigint))
     FROM jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(arr) = 'array' THEN arr ELSE '[]'::jsonb END
          ) AS x
     WHERE x::bigint <> v),
    '[]'::jsonb
  );
$$;

-- Helper: merge a set of ids into a jsonb array (bulk add)
CREATE OR REPLACE FUNCTION {name}.jsonb_ids_merge(arr jsonb, new_ids bigint[])
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT to_jsonb(ARRAY(
    SELECT DISTINCT x FROM (
      SELECT x::bigint AS x
      FROM jsonb_array_elements_text(
             CASE WHEN jsonb_typeof(arr) = 'array' THEN arr ELSE '[]'::jsonb END
           ) AS x
      UNION ALL
      SELECT unnest(new_ids)
    ) s
  ));
$$;

-- Helper: remove a set of ids from a jsonb array (bulk remove)
CREATE OR REPLACE FUNCTION {name}.jsonb_ids_remove_many(arr jsonb, rm_ids bigint[])
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    (SELECT to_jsonb(array_agg(x::bigint))
     FROM jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(arr) = 'array' THEN arr ELSE '[]'::jsonb END
          ) AS x
     WHERE x::bigint <> ALL(rm_ids)),
    '[]'::jsonb
  );
$$;


-- Helper: return the effective timestamp for history entries.
-- Uses the session-local input_date setting if set, otherwise now().
CREATE OR REPLACE FUNCTION {name}._effective_ts()
RETURNS timestamptz
LANGUAGE sql STABLE
AS $$
  SELECT COALESCE(
    nullif(current_setting('{name}.input_date', true), '')::timestamptz,
    now()
  );
$$;


-------------------------------------------------------------------------------
-- INSERT trigger (statement-level, transition table)
-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {name}.object_after_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- Backlinks: for every new row, resolve its forward keys and batch-add
  -- backlink entries on the target rows.
  --
  -- rel_edges joins each new row with its meta record to extract
  -- (source_id, target_id, back_key) triples, then we group by target
  -- and apply a single UPDATE per target.

  WITH rel_edges AS (
    SELECT
      n.id   AS source_id,
      CASE
        WHEN jsonb_typeof(n.attr -> pair.fwd_key) IN ('number','string')
        THEN NULLIF(n.attr ->> pair.fwd_key, '')::bigint
      END AS target_id,
      pair.back_key
    FROM new_rows n
    JOIN {name}.meta mr
      ON mr.category = n.category AND mr.type = n.type AND mr.subtype = n.subtype
    CROSS JOIN LATERAL (
      SELECT key AS fwd_key, value #>> '{}' AS back_key
      FROM jsonb_each(COALESCE(mr.forward, '{}'::jsonb))
    ) pair
    WHERE pair.back_key IS NOT NULL AND pair.back_key <> ''

    UNION ALL

    -- array-valued forward keys
    SELECT
      n.id AS source_id,
      NULLIF(elem, '')::bigint AS target_id,
      pair.back_key
    FROM new_rows n
    JOIN {name}.meta mr
      ON mr.category = n.category AND mr.type = n.type AND mr.subtype = n.subtype
    CROSS JOIN LATERAL (
      SELECT key AS fwd_key, value #>> '{}' AS back_key
      FROM jsonb_each(COALESCE(mr.forward, '{}'::jsonb))
    ) pair
    CROSS JOIN LATERAL (
      SELECT jsonb_array_elements_text(n.attr -> pair.fwd_key) AS elem
      WHERE jsonb_typeof(n.attr -> pair.fwd_key) = 'array'
    ) arr_elems
    WHERE pair.back_key IS NOT NULL AND pair.back_key <> ''
  ),
  -- Group by target + back_key so each target gets ONE update
  grouped AS (
    SELECT target_id, back_key, array_agg(source_id) AS source_ids
    FROM rel_edges
    WHERE target_id IS NOT NULL
    GROUP BY target_id, back_key
  )
  UPDATE {name}.object tgt
  SET attr = jsonb_set(
    tgt.attr,
    ARRAY[g.back_key],
    {name}.jsonb_ids_merge(tgt.attr -> g.back_key, g.source_ids),
    true
  )
  FROM grouped g
  WHERE tgt.id = g.target_id;

  -- History: bulk insert with effective timestamp
  INSERT INTO {name}.history (id, validity, category, type, subtype, attr, deleted, source)
  SELECT n.id, tstzrange({name}._effective_ts(), 'infinity', '(]'),
         n.category, n.type, n.subtype, n.attr, false, n.source
  FROM new_rows n;

  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS object_maintain_trg ON {name}.object;
DROP TRIGGER IF EXISTS object_after_insert_trg ON {name}.object;
CREATE TRIGGER object_after_insert_trg
AFTER INSERT ON {name}.object
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION {name}.object_after_insert();


-------------------------------------------------------------------------------
-- UPDATE trigger (statement-level, transition tables)
-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {name}.object_after_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF pg_trigger_depth() > 1 THEN
    RETURN NULL;
  END IF;

  -- Backlinks: compute per-row old/new target sets, diff them, then batch.
  WITH pairs AS (
    -- Resolve forward-key metadata for both old and new rows.
    -- A row may change type/subtype, so we union the old and new meta.
    SELECT
      o.id,
      pair.fwd_key,
      pair.back_key,
      -- old targets
      CASE
        WHEN jsonb_typeof(o.attr -> pair.fwd_key) IN ('number','string')
        THEN NULLIF(o.attr ->> pair.fwd_key, '')::bigint
      END AS old_scalar,
      CASE
        WHEN jsonb_typeof(n.attr -> pair.fwd_key) IN ('number','string')
        THEN NULLIF(n.attr ->> pair.fwd_key, '')::bigint
      END AS new_scalar
    FROM old_rows o
    JOIN new_rows n ON n.id = o.id
    JOIN {name}.meta mr
      ON mr.category = n.category AND mr.type = n.type AND mr.subtype = n.subtype
    CROSS JOIN LATERAL (
      SELECT key AS fwd_key, value #>> '{}' AS back_key
      FROM jsonb_each(COALESCE(mr.forward, '{}'::jsonb))
    ) pair
    WHERE pair.back_key IS NOT NULL AND pair.back_key <> ''
  ),

  -- Expand scalar forward keys into (id, target_id, back_key, is_old, is_new)
  edges AS (
    -- scalar old
    SELECT id, old_scalar AS target_id, back_key, true AS is_old, false AS is_new
    FROM pairs WHERE old_scalar IS NOT NULL
    UNION ALL
    -- scalar new
    SELECT id, new_scalar AS target_id, back_key, false AS is_old, true AS is_new
    FROM pairs WHERE new_scalar IS NOT NULL
    UNION ALL
    -- array old
    SELECT o.id, NULLIF(elem, '')::bigint AS target_id, pair.back_key, true, false
    FROM old_rows o
    JOIN new_rows n ON n.id = o.id
    JOIN {name}.meta mr
      ON mr.category = n.category AND mr.type = n.type AND mr.subtype = n.subtype
    CROSS JOIN LATERAL (
      SELECT key AS fwd_key, value #>> '{}' AS back_key
      FROM jsonb_each(COALESCE(mr.forward, '{}'::jsonb))
    ) pair
    CROSS JOIN LATERAL (
      SELECT jsonb_array_elements_text(o.attr -> pair.fwd_key) AS elem
      WHERE jsonb_typeof(o.attr -> pair.fwd_key) = 'array'
    ) ae
    WHERE pair.back_key IS NOT NULL AND pair.back_key <> ''
    UNION ALL
    -- array new
    SELECT n.id, NULLIF(elem, '')::bigint AS target_id, pair.back_key, false, true
    FROM old_rows o
    JOIN new_rows n ON n.id = o.id
    JOIN {name}.meta mr
      ON mr.category = n.category AND mr.type = n.type AND mr.subtype = n.subtype
    CROSS JOIN LATERAL (
      SELECT key AS fwd_key, value #>> '{}' AS back_key
      FROM jsonb_each(COALESCE(mr.forward, '{}'::jsonb))
    ) pair
    CROSS JOIN LATERAL (
      SELECT jsonb_array_elements_text(n.attr -> pair.fwd_key) AS elem
      WHERE jsonb_typeof(n.attr -> pair.fwd_key) = 'array'
    ) ae
    WHERE pair.back_key IS NOT NULL AND pair.back_key <> ''
  ),

  -- Aggregate per (source, target, back_key) to determine add vs remove
  diffed AS (
    SELECT id, target_id, back_key,
           bool_or(is_old) AS was_old,
           bool_or(is_new) AS is_new
    FROM edges
    WHERE target_id IS NOT NULL
    GROUP BY id, target_id, back_key
  ),

  -- Targets where we need to ADD this source id
  to_add AS (
    SELECT target_id, back_key, array_agg(id) AS source_ids
    FROM diffed
    WHERE is_new AND NOT was_old
    GROUP BY target_id, back_key
  ),

  -- Targets where we need to REMOVE this source id
  to_remove AS (
    SELECT target_id, back_key, array_agg(id) AS source_ids
    FROM diffed
    WHERE was_old AND NOT is_new
    GROUP BY target_id, back_key
  ),

  -- Apply removals
  do_remove AS (
    UPDATE {name}.object tgt
    SET attr = jsonb_set(
      tgt.attr,
      ARRAY[r.back_key],
      {name}.jsonb_ids_remove_many(tgt.attr -> r.back_key, r.source_ids),
      true
    )
    FROM to_remove r
    WHERE tgt.id = r.target_id
  )

  -- Apply additions
  UPDATE {name}.object tgt
  SET attr = jsonb_set(
    tgt.attr,
    ARRAY[a.back_key],
    {name}.jsonb_ids_merge(tgt.attr -> a.back_key, a.source_ids),
    true
  )
  FROM to_add a
  WHERE tgt.id = a.target_id;

  -- History: validate that effective timestamp is after the start of current validity
  PERFORM 1
  FROM {name}.history h
  JOIN new_rows n ON h.id = n.id
  JOIN old_rows o ON o.id = n.id
  WHERE upper(h.validity) = 'infinity'
    AND {name}._effective_ts() <= lower(h.validity)
    AND (o.category IS DISTINCT FROM n.category
      OR o.type     IS DISTINCT FROM n.type
      OR o.subtype  IS DISTINCT FROM n.subtype
      OR NOT (o.attr @> n.attr AND n.attr @> o.attr));

  IF FOUND THEN
    RAISE EXCEPTION 'input_date (%%) must be strictly after the validity start of the current history entry',
      {name}._effective_ts()
      USING ERRCODE = 'check_violation';
  END IF;

  -- History: close old periods then open new ones (two statements because
  -- the temporal PK constraint needs to see the closed rows before inserting)
  UPDATE {name}.history h
  SET validity = tstzrange(lower(h.validity), {name}._effective_ts(), '(]')
  FROM new_rows n
  JOIN old_rows o ON o.id = n.id
  WHERE h.id = n.id
    AND upper(h.validity) = 'infinity'
    AND (o.category IS DISTINCT FROM n.category
      OR o.type     IS DISTINCT FROM n.type
      OR o.subtype  IS DISTINCT FROM n.subtype
      OR NOT (o.attr @> n.attr AND n.attr @> o.attr));

  INSERT INTO {name}.history (id, validity, category, type, subtype, attr, deleted, source)
  SELECT n.id, tstzrange({name}._effective_ts(), 'infinity', '(]'), n.category, n.type, n.subtype, n.attr, false, n.source
  FROM new_rows n
  JOIN old_rows o ON o.id = n.id
  WHERE o.category IS DISTINCT FROM n.category
     OR o.type     IS DISTINCT FROM n.type
     OR o.subtype  IS DISTINCT FROM n.subtype
     OR NOT (o.attr @> n.attr AND n.attr @> o.attr);

  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS object_after_update_trg ON {name}.object;
CREATE TRIGGER object_after_update_trg
AFTER UPDATE ON {name}.object
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION {name}.object_after_update();


-------------------------------------------------------------------------------
-- DELETE trigger (statement-level, transition table)
-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {name}.object_after_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF pg_trigger_depth() > 1 THEN
    RETURN NULL;
  END IF;

  -- Backlinks: remove deleted ids from all targets' backlink arrays
  WITH rel_edges AS (
    SELECT
      o.id AS source_id,
      CASE
        WHEN jsonb_typeof(o.attr -> pair.fwd_key) IN ('number','string')
        THEN NULLIF(o.attr ->> pair.fwd_key, '')::bigint
      END AS target_id,
      pair.back_key
    FROM old_rows o
    JOIN {name}.meta mr
      ON mr.category = o.category AND mr.type = o.type AND mr.subtype = o.subtype
    CROSS JOIN LATERAL (
      SELECT key AS fwd_key, value #>> '{}' AS back_key
      FROM jsonb_each(COALESCE(mr.forward, '{}'::jsonb))
    ) pair
    WHERE pair.back_key IS NOT NULL AND pair.back_key <> ''

    UNION ALL

    -- array-valued forward keys
    SELECT
      o.id AS source_id,
      NULLIF(elem, '')::bigint AS target_id,
      pair.back_key
    FROM old_rows o
    JOIN {name}.meta mr
      ON mr.category = o.category AND mr.type = o.type AND mr.subtype = o.subtype
    CROSS JOIN LATERAL (
      SELECT key AS fwd_key, value #>> '{}' AS back_key
      FROM jsonb_each(COALESCE(mr.forward, '{}'::jsonb))
    ) pair
    CROSS JOIN LATERAL (
      SELECT jsonb_array_elements_text(o.attr -> pair.fwd_key) AS elem
      WHERE jsonb_typeof(o.attr -> pair.fwd_key) = 'array'
    ) arr_elems
    WHERE pair.back_key IS NOT NULL AND pair.back_key <> ''
  ),
  grouped AS (
    SELECT target_id, back_key, array_agg(source_id) AS source_ids
    FROM rel_edges
    WHERE target_id IS NOT NULL
    GROUP BY target_id, back_key
  )
  UPDATE {name}.object tgt
  SET attr = jsonb_set(
    tgt.attr,
    ARRAY[g.back_key],
    {name}.jsonb_ids_remove_many(tgt.attr -> g.back_key, g.source_ids),
    true
  )
  FROM grouped g
  WHERE tgt.id = g.target_id;

  -- History: validate that effective timestamp is after the start of current validity
  PERFORM 1
  FROM {name}.history h
  JOIN old_rows o ON h.id = o.id
  WHERE upper(h.validity) = 'infinity'
    AND {name}._effective_ts() <= lower(h.validity);

  IF FOUND THEN
    RAISE EXCEPTION 'input_date (%%) must be strictly after the validity start of the current history entry',
      {name}._effective_ts()
      USING ERRCODE = 'check_violation';
  END IF;

  -- History: close validity periods
  UPDATE {name}.history h
  SET validity = tstzrange(lower(h.validity), {name}._effective_ts(), '(]')
  FROM old_rows o
  WHERE h.id = o.id AND upper(h.validity) = 'infinity';

  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS object_delete_trg ON {name}.object;
DROP TRIGGER IF EXISTS object_after_delete_trg ON {name}.object;
CREATE TRIGGER object_after_delete_trg
AFTER DELETE ON {name}.object
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION {name}.object_after_delete();
