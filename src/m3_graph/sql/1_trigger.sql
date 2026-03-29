-- Functions

CREATE OR REPLACE FUNCTION {name}.jsonb_ids_add_unique(arr jsonb, v bigint)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT to_jsonb(ARRAY(
    SELECT DISTINCT x::bigint
    FROM (
      SELECT jsonb_array_elements_text(
               COALESCE(
                 CASE WHEN jsonb_typeof(arr) = 'array' THEN arr ELSE '[]'::jsonb END,
                 '[]'::jsonb
               )
           ) AS x
      UNION ALL
      SELECT v::text
    ) s
  ));
$$;

CREATE OR REPLACE FUNCTION {name}.jsonb_ids_remove(arr jsonb, v bigint)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT to_jsonb(ARRAY(
    SELECT x::bigint
    FROM jsonb_array_elements_text(
           COALESCE(
             CASE WHEN jsonb_typeof(arr) = 'array' THEN arr ELSE '[]'::jsonb END,
             '[]'::jsonb
           )
         ) AS x
    WHERE x::bigint <> v
  ));
$$;

-- Trigger

CREATE OR REPLACE FUNCTION {name}.object_maintain()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  m_old jsonb;
  m_new jsonb;
  pair record;
BEGIN
  IF pg_trigger_depth() > 1 THEN
    RETURN NEW;
  END IF;

  -- mapping for OLD type (if update)
  IF TG_OP = 'UPDATE' THEN
    SELECT mr.forward INTO m_old
    FROM {name}.meta mr
    WHERE mr.category = OLD.category AND mr.type = OLD.type AND mr.subtype = OLD.subtype;

    IF m_old IS NULL THEN
      m_old := '{}'::jsonb;
    END IF;
  ELSE
    m_old := '{}'::jsonb;
  END IF;

  -- mapping for NEW type
  SELECT mr.forward INTO m_new
  FROM {name}.meta mr
  WHERE mr.category = NEW.category AND mr.type = NEW.type AND mr.subtype = NEW.subtype;

  IF m_new IS NULL THEN
    m_new := '{}'::jsonb;
  END IF;

  /*
    Iterate over all keys that appear in either mapping.
    Each key maps to a backlink attribute name (text) or null.
    If null => ignore backlink maintenance for that key.
  */
  FOR pair IN
    WITH keys AS (
      SELECT DISTINCT k
      FROM (
        SELECT jsonb_object_keys(m_old) AS k
        UNION ALL
        SELECT jsonb_object_keys(m_new) AS k
      ) s
    )
    SELECT
      k AS fwd_key,
      COALESCE(m_new ->> k, m_old ->> k) AS back_key
    FROM keys
  LOOP
    -- ignore if no backlink attribute specified
    IF pair.back_key IS NULL OR pair.back_key = '' THEN
      CONTINUE;
    END IF;

    -- remove backlinks from targets no longer referenced
    WITH
      old_ids AS (
        SELECT DISTINCT ref_id
        FROM (
          -- scalar old
          SELECT CASE
                   WHEN TG_OP = 'UPDATE'
                    AND jsonb_typeof(OLD.attr -> pair.fwd_key) IN ('number','string')
                   THEN NULLIF(OLD.attr ->> pair.fwd_key, '')::bigint
                 END AS ref_id
          UNION ALL
          -- array old
          SELECT NULLIF(x.elem, '')::bigint
          FROM LATERAL (
            SELECT jsonb_array_elements_text(OLD.attr -> pair.fwd_key) AS elem
            WHERE TG_OP = 'UPDATE'
              AND jsonb_typeof(OLD.attr -> pair.fwd_key) = 'array'
          ) x
        ) s
        WHERE ref_id IS NOT NULL
      ),
      new_ids AS (
        SELECT DISTINCT ref_id
        FROM (
          -- scalar new
          SELECT CASE
                   WHEN jsonb_typeof(NEW.attr -> pair.fwd_key) IN ('number','string')
                   THEN NULLIF(NEW.attr ->> pair.fwd_key, '')::bigint
                 END AS ref_id
          UNION ALL
          -- array new
          SELECT NULLIF(x.elem, '')::bigint
          FROM LATERAL (
            SELECT jsonb_array_elements_text(NEW.attr -> pair.fwd_key) AS elem
            WHERE jsonb_typeof(NEW.attr -> pair.fwd_key) = 'array'
          ) x
        ) s
        WHERE ref_id IS NOT NULL
      ),
      to_remove AS (
        SELECT ref_id FROM old_ids
        EXCEPT
        SELECT ref_id FROM new_ids
      )
    UPDATE {name}.object tgt
    SET attr = jsonb_set(
      tgt.attr,
      ARRAY[pair.back_key],
      {name}.jsonb_ids_remove(tgt.attr -> pair.back_key, NEW.id),
      true
    )
    WHERE tgt.id IN (SELECT ref_id FROM to_remove);

    -- add backlinks on newly referenced targets
    WITH
      old_ids AS (
        SELECT DISTINCT ref_id
        FROM (
          -- scalar old
          SELECT CASE
                   WHEN TG_OP = 'UPDATE'
                    AND jsonb_typeof(OLD.attr -> pair.fwd_key) IN ('number','string')
                   THEN NULLIF(OLD.attr ->> pair.fwd_key, '')::bigint
                 END AS ref_id
          UNION ALL
          -- array old
          SELECT NULLIF(x.elem, '')::bigint
          FROM LATERAL (
            SELECT jsonb_array_elements_text(OLD.attr -> pair.fwd_key) AS elem
            WHERE TG_OP = 'UPDATE'
              AND jsonb_typeof(OLD.attr -> pair.fwd_key) = 'array'
          ) x
        ) s
        WHERE ref_id IS NOT NULL
      ),
      new_ids AS (
        SELECT DISTINCT ref_id
        FROM (
          -- scalar new
          SELECT CASE
                   WHEN jsonb_typeof(NEW.attr -> pair.fwd_key) IN ('number','string')
                   THEN NULLIF(NEW.attr ->> pair.fwd_key, '')::bigint
                 END AS ref_id
          UNION ALL
          -- array new
          SELECT NULLIF(x.elem, '')::bigint
          FROM LATERAL (
            SELECT jsonb_array_elements_text(NEW.attr -> pair.fwd_key) AS elem
            WHERE jsonb_typeof(NEW.attr -> pair.fwd_key) = 'array'
          ) x
        ) s
        WHERE ref_id IS NOT NULL
      ),
      to_add AS (
        SELECT ref_id FROM new_ids
        EXCEPT
        SELECT ref_id FROM old_ids
      )
    UPDATE {name}.object tgt
    SET attr = jsonb_set(
      tgt.attr,
      ARRAY[pair.back_key],
      {name}.jsonb_ids_add_unique(tgt.attr -> pair.back_key, NEW.id),
      true
    )
    WHERE tgt.id IN (SELECT ref_id FROM to_add);

  END LOOP;

  -- history (only for the initiating write)
  IF TG_OP = 'INSERT' THEN
    -- Insert new history row with open-ended validity
    INSERT INTO {name}.history (id, category, type, subtype, attr, deleted, source)
    VALUES (NEW.id, NEW.category, NEW.type, NEW.subtype, NEW.attr, false, NEW.source);
  ELSIF OLD.category IS DISTINCT FROM NEW.category
     OR OLD.type     IS DISTINCT FROM NEW.type
     OR OLD.subtype  IS DISTINCT FROM NEW.subtype
     OR NOT (OLD.attr @> NEW.attr AND NEW.attr @> OLD.attr)
  THEN
    -- Close the current validity period by setting upper bound to now()
    UPDATE {name}.history
    SET validity = tstzrange(lower(validity), now(), '(]')
    WHERE id = NEW.id
      AND upper(validity) = 'infinity';

    -- Insert new history row with validity starting from now()
    INSERT INTO {name}.history (id, validity, category, type, subtype, attr, deleted, source)
    VALUES (NEW.id, tstzrange(now(), 'infinity', '(]'), NEW.category, NEW.type, NEW.subtype, NEW.attr, false, NEW.source);
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS object_maintain_trg ON {name}.object;
CREATE TRIGGER object_maintain_trg
AFTER INSERT OR UPDATE OF category, type, subtype, attr
ON {name}.object
FOR EACH ROW
EXECUTE FUNCTION {name}.object_maintain();


CREATE OR REPLACE FUNCTION {name}.object_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  m jsonb;
  pair record;
BEGIN
  IF pg_trigger_depth() > 1 THEN
    RETURN OLD;
  END IF;

  SELECT mr.forward INTO m
  FROM {name}.meta mr
  WHERE mr.category = OLD.category AND mr.type = OLD.type AND mr.subtype = OLD.subtype;

  IF m IS NULL THEN
    m := '{}'::jsonb;
  END IF;

  FOR pair IN
    SELECT key AS fwd_key, value AS back_key
    FROM jsonb_each_text(m)
  LOOP
    IF pair.back_key IS NULL OR pair.back_key = '' THEN
      CONTINUE;
    END IF;

    WITH old_ids AS (
      SELECT DISTINCT ref_id
      FROM (
        SELECT CASE
                 WHEN jsonb_typeof(OLD.attr -> pair.fwd_key) IN ('number','string')
                 THEN NULLIF(OLD.attr ->> pair.fwd_key, '')::bigint
               END AS ref_id
        UNION ALL
        SELECT NULLIF(x.elem,'')::bigint
        FROM LATERAL (
          SELECT jsonb_array_elements_text(OLD.attr -> pair.fwd_key) AS elem
          WHERE jsonb_typeof(OLD.attr -> pair.fwd_key) = 'array'
        ) x
      ) s
      WHERE ref_id IS NOT NULL
    )
    UPDATE {name}.object tgt
    SET attr = jsonb_set(
      tgt.attr,
      ARRAY[pair.back_key],
      {name}.jsonb_ids_remove(tgt.attr -> pair.back_key, OLD.id),
      true
    )
    WHERE tgt.id IN (SELECT ref_id FROM old_ids);
  END LOOP;

  UPDATE {name}.history
  SET validity = tstzrange(lower(validity), now(), '(]')
  WHERE id = OLD.id
    AND upper(validity) = 'infinity';

  RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS object_delete_trg ON {name}.object;
CREATE TRIGGER object_delete_trg
AFTER DELETE ON {name}.object
FOR EACH ROW
EXECUTE FUNCTION {name}.object_delete();
