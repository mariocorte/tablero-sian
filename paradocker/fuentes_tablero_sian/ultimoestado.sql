-- Crea un índice que optimiza la búsqueda del último estado por movimiento,
-- actuación y domicilio electrónico.
CREATE INDEX IF NOT EXISTS idx_notpolhistoricomp_mov_act_dom_fecha
    ON public.notpolhistoricomp (pmovimientoid, pactuacionid, pdomicilioelectronicopj, notpolhistoricompfecha DESC);

-- Función que devuelve la fecha y el estado más reciente para los parámetros indicados.
CREATE OR REPLACE FUNCTION public.obtener_ultimo_estado(
    p_pmovimientoid double precision,
    p_pactuacionid numeric,
    p_pdomicilioelectronicopj varchar
)
RETURNS TABLE (
    notpolhistoricompfecha varchar,
    notpolhistoricompestado varchar
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        n.notpolhistoricompfecha,
        n.notpolhistoricompestado
    FROM public.notpolhistoricomp AS n
    WHERE n.pmovimientoid = p_pmovimientoid
      AND n.pactuacionid = p_pactuacionid
      AND n.pdomicilioelectronicopj = p_pdomicilioelectronicopj
    ORDER BY n.notpolhistoricompfecha DESC
    LIMIT 1;
END;
$$;
