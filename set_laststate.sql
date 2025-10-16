-- Crea un índice parcial para acelerar la búsqueda de registros pendientes de
-- actualización (sin fecha de último estado establecida).
CREATE INDEX IF NOT EXISTS idx_envio_notif_pol_fechalaststate_null
    ON public.enviocedulanotificacionpolicia (pmovimientoid, pactuacionid, pdomicilioelectronicopj)
    WHERE fechalaststate IS NULL;

-- Actualiza los campos laststagesian y laststate con la información devuelta
-- por la función obtener_ultimo_estado para los registros que aún no poseen
-- fecha de último estado.
UPDATE public.enviocedulanotificacionpolicia AS e
SET laststagesian = ultimo.notpolhistoricompestado,
    laststate = ultimo.notpolhistoricompfecha
FROM LATERAL (
    SELECT u.notpolhistoricompfecha, u.notpolhistoricompestado
    FROM public.obtener_ultimo_estado(
        e.pmovimientoid,
        e.pactuacionid,
        e.pdomicilioelectronicopj
    ) AS u
) AS ultimo
WHERE e.fechalaststate IS NULL;
