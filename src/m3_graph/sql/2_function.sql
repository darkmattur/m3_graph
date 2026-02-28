CREATE OR REPLACE FUNCTION {name}.fetch_object(root_ids bigint[])
RETURNS TABLE (id bigint, category text, type text, subtype text, attr jsonb, source text)
LANGUAGE sql
STABLE
AS $$
WITH RECURSIVE graph AS (
  SELECT o.id, o.category, o.type, o.subtype, o.attr, o.source
  FROM {name}.object o
  WHERE o.id = ANY(root_ids)

  UNION

  SELECT o2.id, o2.category, o2.type, o2.subtype, o2.attr, o2.source
  FROM graph g
  LEFT JOIN {name}.meta mr
    ON mr.category = g.category
   AND mr.type     = g.type
   AND mr.subtype  = g.subtype

  JOIN LATERAL (
    SELECT DISTINCT next_id
    FROM (
      -- forward (from relationship keys)
      SELECT CASE
               WHEN jsonb_typeof(g.attr -> k) IN ('number','string')
               THEN NULLIF(g.attr ->> k, '')::bigint
             END AS next_id
      FROM jsonb_object_keys(COALESCE(mr.forward, '{}'::jsonb)) AS k

      UNION ALL

      SELECT NULLIF(x.elem,'')::bigint
      FROM jsonb_object_keys(COALESCE(mr.forward, '{}'::jsonb)) AS k
      JOIN LATERAL (
        SELECT jsonb_array_elements_text(g.attr -> k) AS elem
        WHERE jsonb_typeof(g.attr -> k) = 'array'
      ) x ON TRUE

      -- backward (from mr.back[])
      UNION ALL

      SELECT NULLIF(x.elem,'')::bigint
      FROM unnest(COALESCE(mr.back, '{}'::text[])) AS bk
      JOIN LATERAL (
        SELECT jsonb_array_elements_text(g.attr -> bk) AS elem
        WHERE jsonb_typeof(g.attr -> bk) = 'array'
      ) x ON TRUE
    ) u
    WHERE next_id IS NOT NULL
  ) nxt ON TRUE

  JOIN {name}.object o2 ON o2.id = nxt.next_id
)
SELECT id, category, type, subtype, attr, source
FROM graph;
$$;

-- Fetch objects by type, including descendant types, optionally expanding through relationships
CREATE OR REPLACE FUNCTION {name}.fetch_object_by_type(target_type text, expand boolean DEFAULT false)
RETURNS TABLE (id bigint, category text, type text, subtype text, attr jsonb, source text)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  IF expand THEN
    RETURN QUERY
    SELECT f.*
    FROM {name}.fetch_object(
      ARRAY(
        SELECT o.id
        FROM {name}.object o
        WHERE o.type = target_type
           OR EXISTS (
             SELECT 1 FROM {name}.meta m
             WHERE m.type = o.type
               AND target_type = ANY(m.parent_types)
           )
      )
    ) f
    ORDER BY f.id;
  ELSE
    RETURN QUERY
    SELECT o.id, o.category, o.type, o.subtype, o.attr, o.source
    FROM {name}.object o
    WHERE o.type = target_type
       OR EXISTS (
         SELECT 1 FROM {name}.meta m
         WHERE m.type = o.type
           AND target_type = ANY(m.parent_types)
       )
    ORDER BY o.id;
  END IF;
END;
$$;
