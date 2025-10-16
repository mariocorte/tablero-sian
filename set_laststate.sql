-- Crea un índice parcial para acelerar la búsqueda de registros pendientes de
-- actualización (sin fecha de último estado establecida).
CREATE INDEX IF NOT EXISTS idx_envio_notif_pol_fechalaststate_null
    ON public.enviocedulanotificacionpolicia (pmovimientoid, pactuacionid, pdomicilioelectronicopj)
    WHERE fechalaststate IS NULL;

WITH datos AS (
    SELECT
        e.pmovimientoid,
        e.pactuacionid,
        e.pdomicilioelectronicopj,
        ultimo.notpolhistoricompestado,
        ultimo.notpolhistoricompfecha
    FROM public.enviocedulanotificacionpolicia AS e
    CROSS JOIN LATERAL public.obtener_ultimo_estado(
        e.pmovimientoid,
        e.pactuacionid,
        e.pdomicilioelectronicopj
    ) AS ultimo
    WHERE e.fechalaststate IS NULL
)
UPDATE public.enviocedulanotificacionpolicia AS e
SET laststagesian = datos.notpolhistoricompestado,
    laststate = datos.notpolhistoricompfecha
FROM datos
WHERE e.pmovimientoid = datos.pmovimientoid
  AND e.pactuacionid = datos.pactuacionid
  AND e.pdomicilioelectronicopj = datos.pdomicilioelectronicopj
  AND e.fechalaststate IS NULL;
