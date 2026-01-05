SELECT *
FROM public.enviocedulanotificacionpolicia AS env
WHERE env.codigoseguimientomp IS NULL
  AND COALESCE(env.descartada, false) = false
  AND EXISTS (
    SELECT 1
    FROM public.enviocedulanotificacionpolicia AS env2
    WHERE env2.pactuacionid = env.pactuacionid
      AND env2.pdomicilioelectronicopj <> env.pdomicilioelectronicopj
      AND env2.codigoseguimientomp IS NOT NULL
  );

UPDATE public.enviocedulanotificacionpolicia AS env
SET descartada = TRUE,
    fechadescarte = CURRENT_TIMESTAMP,
    usuariodescarte = 8880
WHERE env.codigoseguimientomp IS NULL
  AND COALESCE(env.descartada, false) = false
  AND EXISTS (
    SELECT 1
    FROM public.enviocedulanotificacionpolicia AS env2
    WHERE env2.pactuacionid = env.pactuacionid
      AND env2.pdomicilioelectronicopj <> env.pdomicilioelectronicopj
      AND env2.codigoseguimientomp IS NOT NULL
  );
