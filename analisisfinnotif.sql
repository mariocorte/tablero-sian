-- Busca actuaciones con estados registrados luego de haber marcado "Entregada".
-- El análisis agrupa los movimientos por movimiento, actuación y domicilio electrónico.
WITH historico AS (
    SELECT
        n.notpolhistoricompestadonid,
        n.pmovimientoid,
        n.pactuacionid,
        n.pdomicilioelectronicopj,
        n.notpolhistoricompfecha,
        n.notpolhistoricompestado,
        MAX(CASE WHEN n.notpolhistoricompestado ILIKE 'Entregada' THEN n.notpolhistoricompestadonid END)
            OVER (
                PARTITION BY n.pmovimientoid, n.pactuacionid, n.pdomicilioelectronicopj
            ) AS ultima_entregada_id
    FROM public.notpolhistoricomp AS n
),
con_estados_posteriores AS (
    SELECT DISTINCT
        h.pmovimientoid,
        h.pactuacionid,
        h.pdomicilioelectronicopj,
        h.ultima_entregada_id
    FROM historico AS h
    WHERE h.ultima_entregada_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM historico AS posteriores
        WHERE posteriores.pmovimientoid = h.pmovimientoid
          AND posteriores.pactuacionid = h.pactuacionid
          AND COALESCE(posteriores.pdomicilioelectronicopj, '') = COALESCE(h.pdomicilioelectronicopj, '')
          AND posteriores.notpolhistoricompestadonid > h.ultima_entregada_id
      )
)
SELECT
    posteriores.pmovimientoid,
    posteriores.pactuacionid,
    posteriores.pdomicilioelectronicopj,
    con_estados_posteriores.ultima_entregada_id,
    posteriores.notpolhistoricompestadonid AS estado_posterior_id,
    posteriores.notpolhistoricompfecha,
    posteriores.notpolhistoricompestado
FROM con_estados_posteriores
JOIN historico AS posteriores
  ON posteriores.pmovimientoid = con_estados_posteriores.pmovimientoid
 AND posteriores.pactuacionid = con_estados_posteriores.pactuacionid
 AND COALESCE(posteriores.pdomicilioelectronicopj, '') = COALESCE(con_estados_posteriores.pdomicilioelectronicopj, '')
WHERE posteriores.notpolhistoricompestadonid > con_estados_posteriores.ultima_entregada_id
ORDER BY
    posteriores.pmovimientoid,
    posteriores.pactuacionid,
    posteriores.pdomicilioelectronicopj,
    posteriores.notpolhistoricompestadonid;
