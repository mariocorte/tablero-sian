WITH act_pol AS (
    SELECT
        A.exp_id,
        A.act_id,
        A.act_numero,
        A.act_fecfir
    FROM act AS A
    INNER JOIN dac AS D ON A.dac_id = D.dac_id
    INNER JOIN uje_act AS UA ON A.act_id = UA.act_id AND A.exp_id = UA.exp_id
    WHERE D.dac_cod IN ('CEDPOL', 'CEDCIT', 'CEDCON', 'CEDURG')
        AND (A.act_estrecep = 1 OR A.act_estrecep IS NULL)
        AND A.eac_id IN (6, 29)
        AND UA.es_enotif = 0
        AND A.act_fecfir IS NOT NULL
        AND date_trunc('minute', A.act_fecfir) >= date_trunc('minute', CURRENT_TIMESTAMP - INTERVAL '14 days')
    GROUP BY A.exp_id, A.act_id, A.act_numero, A.act_fecfir
)
SELECT DISTINCT
    0 AS movimientoId,
    CASE
        WHEN length(A.act_numero::varchar) = 8 THEN trim('9000' || A.act_numero::varchar)
        WHEN length(A.act_numero::varchar) = 9 THEN trim('900' || A.act_numero::varchar)
        WHEN length(A.act_numero::varchar) = 10 THEN trim('90' || A.act_numero::varchar)
        WHEN length(A.act_numero::varchar) = 11 THEN trim('9' || A.act_numero::varchar)
    END AS actuacionidirx,
    A.act_id AS actuacionId,
    TE.tex_cod AS documentoTipoAbreviatura,
    E.exp_numero,
    E.exp_anio,
    A.act_fecfir AS actuacionFechaFirma,
    CASE
        WHEN D.tac_id = 60 THEN D.dac_descr
        ELSE A.act_titulo
    END AS Titulo,
    A.act_obs AS Observaciones,
    OI.org_id_mp AS iddependenciaEnvioPJ,
    OI.org_cod AS coddependenciaEnvioPJ,
    OI.org_descr AS dependenciaEnvioNombre,
    6 AS IdCentroNotificacion,
    CASE
        WHEN D.dac_cod = 'CEDURG' THEN 5
        WHEN D.dac_cod = 'CEDCIT' THEN 1
        WHEN D.dac_cod = 'CEDCON' THEN 6
        ELSE 4
    END AS IdTipoNotificacion,
    4 AS IdSistema,
    'false' AS EnvioFisico,
    REGEXP_REPLACE(E.exp_carat, '<patronTxt>', '', 'g') AS DescripcionCausa,
    (
        SELECT
            CASE
                WHEN TD.tdo_cod = 'SN' THEN 'SN - SIN NUMERO DE DOCUMENTO'
                WHEN TD.tdo_cod <> 'SN' THEN TD.tdo_cod || ' : ' || P.per_nrodoc
            END
        FROM per AS P
        INNER JOIN tdo AS TD ON P.tdo_id = TD.tdo_id
        WHERE P.per_id = UA.per_id
    ) AS documento,
    UA.destino_notif AS Destinatario,
    UA.dir_notif AS DireccionDestinatario,
    NULL AS FechaYHoraAudiencia,
    A.act_numero AS CedulaNumero,
    OI.org_cod AS domicilioElectronicoPJ,
    'POLICIA SALTA - DIV. ASUNTOS JUDICIALES' AS representado,
    D.dac_cod,
    D.dac_descr,
    A.act_pdf AS actuacionArchivoPdf,
    TE.tex_cod AS irx_tcc_codigo,
    E.exp_numero AS irx_hca_numero,
    E.exp_anio AS irx_hca_anio,
    D.dac_cod AS irx_dac_codigo,
    CASE
        WHEN length(A.act_numero::varchar) = 8 THEN trim('9000' || A.act_numero::varchar)
        WHEN length(A.act_numero::varchar) = 9 THEN trim('900' || A.act_numero::varchar)
        WHEN length(A.act_numero::varchar) = 10 THEN trim('90' || A.act_numero::varchar)
        WHEN length(A.act_numero::varchar) = 11 THEN trim('9' || A.act_numero::varchar)
    END AS irx_hac_numero,
    (
        SELECT string_agg(AA.aac_nombre, '; ')
        FROM aac AS AA
        WHERE A.act_id = AA.act_id
    ) AS adjuntoNombres,
    '' AS adjuntoArchivos,
    A.act_idrel AS IdActOrigen,
    (
        SELECT AO.act_pdf
        FROM act AS AO
        WHERE A.act_idrel = AO.act_id
    ) AS ArchivosActOrigen,
    (
        SELECT string_agg(U.usr_nombre, ',')
        FROM act_fir AS AF
        INNER JOIN usr AS U ON U.usr_id = AF.usr_id
        WHERE A.act_idrel = AF.act_id
    ) AS fte_resolucion,
    E.id_denuncia AS denuncia_id
FROM act AS A
INNER JOIN act_pol AS AP ON A.exp_id = AP.exp_id
INNER JOIN uje_act AS UA ON A.act_id = UA.act_id AND A.exp_id = UA.exp_id
INNER JOIN usr AS U ON UA.usr_id = U.usr_id
INNER JOIN exp AS E ON A.exp_id = E.exp_id
INNER JOIN tex AS TE ON E.tex_id = TE.tex_id
INNER JOIN org AS OI ON E.org_idradactual = OI.org_id
INNER JOIN dac AS D ON A.dac_id = D.dac_id
WHERE D.dac_cod IN ('CEDPOL', 'CEDCIT', 'CEDCON', 'CEDURG')
    AND (A.act_estrecep = 1 OR A.act_estrecep IS NULL)
    AND A.eac_id IN (6, 29)
    AND A.act_fecfir IS NOT NULL
    AND date_trunc('minute', A.act_fecfir) >= date_trunc('minute', CURRENT_TIMESTAMP - INTERVAL '14 days')
    AND UA.es_enotif = 0;
