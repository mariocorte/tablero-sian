-- Reporta diferencias entre el último estado de MP y el registrado en SIAN.
-- Incluye los casos en que no existe historial en notpolhistoricomp.
WITH ultimo_estado AS (
    SELECT DISTINCT ON (TRIM(codigoseguimientomp))
        TRIM(codigoseguimientomp) AS codigoseguimientomp,
        notpolhistoricompfecha,
        notpolhistoricompestado
    FROM public.notpolhistoricomp
    WHERE codigoseguimientomp IS NOT NULL
      AND TRIM(codigoseguimientomp) <> ''
    ORDER BY TRIM(codigoseguimientomp),
             to_timestamp(
                 left(replace(notpolhistoricompfecha, 'T', ' '), 19),
                 'YYYY-MM-DD HH24:MI:SS'
             ) DESC NULLS LAST,
             notpolhistoricompestadonid DESC NULLS LAST
)
SELECT
    TRIM(env.codigoseguimientomp) AS codigoseguimientomp,
    env.laststagesian,
    ultimo_estado.notpolhistoricompestado AS ultimo_estado_notpolhistoricomp,
    env.penviocedulanotificacionfechahora,
    env.pfechayhora,
    env.pfechahora,
    env.fechacreacion,
    env.fechalaststate,
    ultimo_estado.notpolhistoricompfecha
FROM public.enviocedulanotificacionpolicia AS env
LEFT JOIN ultimo_estado
  ON TRIM(env.codigoseguimientomp) = ultimo_estado.codigoseguimientomp
WHERE env.codigoseguimientomp IS NOT NULL
  AND TRIM(env.codigoseguimientomp) <> ''
  AND (
    ultimo_estado.codigoseguimientomp IS NULL
    OR COALESCE(env.laststagesian, '') <> COALESCE(ultimo_estado.notpolhistoricompestado, '')
  )
ORDER BY TRIM(env.codigoseguimientomp);
