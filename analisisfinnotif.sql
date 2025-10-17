-- Busca actuaciones con estados registrados luego de haber marcado "Entregada".
-- El análisis agrupa los movimientos por movimiento, actuación y domicilio electrónico.
WITH historico AS (
    SELECT
        n.pmovimientoid,
        n.pactuacionid,
        n.pdomicilioelectronicopj,
        n.notpolhistoricompfecha,
        n.notpolhistoricompestado,
        n.notpolhistoricompestadonid,
        MAX(CASE WHEN n.notpolhistoricompestado ILIKE 'Entregada' THEN n.notpolhistoricompfecha END)
            OVER (
                PARTITION BY n.pmovimientoid, n.pactuacionid, n.pdomicilioelectronicopj
            ) AS ultima_entregada_fecha
    FROM public.notpolhistoricomp AS n
),
con_estados_posteriores AS (
    SELECT DISTINCT
        h.pmovimientoid,
        h.pactuacionid,
        h.pdomicilioelectronicopj,
        h.ultima_entregada_fecha
    FROM historico AS h
    WHERE h.ultima_entregada_fecha IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM historico AS posteriores
        WHERE posteriores.pmovimientoid = h.pmovimientoid
          AND posteriores.pactuacionid = h.pactuacionid
          AND COALESCE(posteriores.pdomicilioelectronicopj, '') = COALESCE(h.pdomicilioelectronicopj, '')
          AND posteriores.notpolhistoricompfecha > h.ultima_entregada_fecha
      )
)
SELECT
    posteriores.pmovimientoid,
    posteriores.pactuacionid,
    posteriores.pdomicilioelectronicopj,
    con_estados_posteriores.ultima_entregada_fecha,
    posteriores.notpolhistoricompestadonid AS estado_posterior_id,
    posteriores.notpolhistoricompfecha,
    posteriores.notpolhistoricompestado
FROM con_estados_posteriores
JOIN historico AS posteriores
 ON posteriores.pmovimientoid = con_estados_posteriores.pmovimientoid
 AND posteriores.pactuacionid = con_estados_posteriores.pactuacionid
 AND COALESCE(posteriores.pdomicilioelectronicopj, '') = COALESCE(con_estados_posteriores.pdomicilioelectronicopj, '')
WHERE posteriores.notpolhistoricompfecha > con_estados_posteriores.ultima_entregada_fecha
ORDER BY
    posteriores.pmovimientoid,
    posteriores.pactuacionid,
    posteriores.pdomicilioelectronicopj,
    posteriores.notpolhistoricompfecha;
