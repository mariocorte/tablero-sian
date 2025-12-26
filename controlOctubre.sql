-- Lista los registros cuyo primer estado fue en octubre de 2025 (Pendiente)
-- y devuelve el primer y último estado por codigo de seguimiento.
WITH historico AS (
    SELECT
        n.codigoseguimientomp,
        n.notpolhistoricompfecha,
        n.notpolhistoricompestado,
        to_timestamp(
            left(replace(n.notpolhistoricompfecha, 'T', ' '), 19),
            'YYYY-MM-DD HH24:MI:SS'
        ) AS notpolhistoricompfecha_ts
    FROM public.notpolhistoricomp AS n
),
ordenado AS (
    SELECT
        h.*,
        row_number() OVER (
            PARTITION BY h.codigoseguimientomp
            ORDER BY h.notpolhistoricompfecha_ts ASC NULLS LAST,
                     h.notpolhistoricompestadonid ASC NULLS LAST
        ) AS rn_asc,
        row_number() OVER (
            PARTITION BY h.codigoseguimientomp
            ORDER BY h.notpolhistoricompfecha_ts DESC NULLS LAST,
                     h.notpolhistoricompestadonid DESC NULLS LAST
        ) AS rn_desc
    FROM historico AS h
),
primeros AS (
    SELECT
        o.codigoseguimientomp,
        o.notpolhistoricompfecha AS fecha_primer_estado,
        o.notpolhistoricompestado AS primer_estado
    FROM ordenado AS o
    WHERE o.rn_asc = 1
),
ultimos AS (
    SELECT
        o.codigoseguimientomp,
        o.notpolhistoricompfecha AS fecha_ultimo_estado,
        o.notpolhistoricompestado AS ultimo_estado
    FROM ordenado AS o
    WHERE o.rn_desc = 1
)
SELECT
    p.codigoseguimientomp,
    p.fecha_primer_estado,
    p.primer_estado,
    u.fecha_ultimo_estado,
    u.ultimo_estado
FROM primeros AS p
JOIN ultimos AS u
    ON u.codigoseguimientomp = p.codigoseguimientomp
WHERE p.primer_estado = 'Pendiente'
  AND p.fecha_primer_estado::date >= DATE '2025-10-01'
  AND p.fecha_primer_estado::date < DATE '2025-11-01'
ORDER BY p.fecha_primer_estado::date,
         p.codigoseguimientomp;
