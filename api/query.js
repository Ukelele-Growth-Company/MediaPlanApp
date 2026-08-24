// Serverless API (Vercel) — corre queries de solo lectura a BigQuery.
// Auth: cuenta de servicio en la env var GCP_SA_KEY (JSON).
// Acceso: opcional, clave compartida en APP_KEY (header x-app-key).
const { BigQuery } = require('@google-cloud/bigquery');

const PROJECT = process.env.GCP_PROJECT || 'gen-lang-client-0913063106';
const CAMP = '`' + PROJECT + '.prune_cl.master_campaign_results`';
const ADSET = '`' + PROJECT + '.prune_cl.master_ads_ad_results`';
const BUD = '`' + PROJECT + '.prune_cl.media_plan_budgets`';
const PLAT = "platform IN ('Facebook','Google')";
const PCASE = "CASE platform WHEN 'Facebook' THEN 'meta' WHEN 'Google' THEN 'gads' END";

// Whitelist de queries. El cliente NUNCA manda SQL: manda un "kind" + params.
const QUERIES = {
  pacing_campaigns: p => ({
    query: `SELECT ${PCASE} grp, campaign_name name, SUM(cost_converted) spend
            FROM ${CAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to
            GROUP BY grp, name HAVING spend > 0`,
    params: { from: p.from, to: p.to }
  }),
  pacing_daily: p => ({
    query: `SELECT CAST(date AS STRING) date, ${PCASE} grp, SUM(cost_converted) spend
            FROM ${CAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to
            GROUP BY date, grp`,
    params: { from: p.from, to: p.to }
  }),
  ga4_totals: p => ({
    query: `SELECT SUM(purchase_revenue_converted) rev, SUM(purchase) tx, SUM(session_start) sess
            FROM ${CAMP} WHERE date BETWEEN @from AND @to`,
    params: { from: p.from, to: p.to }
  }),
  results: p => ({
    query: `SELECT ${PCASE} grp, campaign_name name,
              SUM(cost_converted) cons, SUM(impressions) imp, SUM(clicks) clk,
              SUM(purchase_revenue_converted) ga4Rev, SUM(purchase) ga4Tx, SUM(session_start) sess,
              SUM(ads_purchase_revenue_converted) plRev, SUM(ads_purchase) plTx
            FROM ${CAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to
            GROUP BY grp, name HAVING cons > 0 OR ga4Rev > 0 OR plRev > 0`,
    params: { from: p.from, to: p.to }
  }),
  camp_daily: p => ({
    query: `SELECT CAST(date AS STRING) date,
              SUM(cost_converted) cons, SUM(impressions) imp, SUM(clicks) clk,
              SUM(purchase_revenue_converted) ga4Rev, SUM(purchase) ga4Tx, SUM(session_start) sess,
              SUM(ads_purchase_revenue_converted) plRev, SUM(ads_purchase) plTx
            FROM ${CAMP} WHERE campaign_name = @name AND ${PLAT} AND date BETWEEN @from AND @to
            GROUP BY date ORDER BY date`,
    params: { name: p.name, from: p.from, to: p.to }
  }),
  adsets: p => ({
    query: `SELECT ad_set_name name,
              SUM(cost_converted) cons, SUM(impressions) imp, SUM(clicks) clk,
              SUM(ads_purchase_revenue_converted) plRev, SUM(ads_purchase) plTx
            FROM ${ADSET} WHERE campaign_name = @name AND date BETWEEN @from AND @to
            GROUP BY ad_set_name HAVING cons > 0 ORDER BY cons DESC`,
    params: { name: p.name, from: p.from, to: p.to }
  }),
  adset_daily: p => ({
    query: `SELECT CAST(date AS STRING) date,
              SUM(cost_converted) cons, SUM(impressions) imp, SUM(clicks) clk,
              SUM(ads_purchase_revenue_converted) plRev, SUM(ads_purchase) plTx
            FROM ${ADSET} WHERE campaign_name = @camp AND ad_set_name = @adset AND date BETWEEN @from AND @to
            GROUP BY date ORDER BY date`,
    params: { camp: p.camp, adset: p.adset, from: p.from, to: p.to }
  }),
  // --- plan mensual (presupuesto por campaña), compartido por todo el equipo ---
  active_campaigns: p => ({ query: `SELECT DISTINCT ${PCASE} grp, campaign_name name FROM ${CAMP} WHERE ${PLAT} AND cost_converted>0 AND date=(SELECT MAX(date) FROM ${CAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to)`, params: { from: p.from, to: p.to } }), active_adsets: p => ({ query: `SELECT DISTINCT ad_set_name name FROM ${ADSET} WHERE campaign_name=@name AND cost_converted>0 AND date=(SELECT MAX(date) FROM ${ADSET} WHERE campaign_name=@name AND date BETWEEN @from AND @to)`, params: { name: p.name, from: p.from, to: p.to } }), budgets_get: p => ({
    query: `SELECT platform, campaign, amount FROM ${BUD} WHERE month = @month`,
    params: { month: p.month }
  }),
  budget_set: p => ({
    query: `MERGE ${BUD} T
            USING (SELECT @month month, @platform platform, @campaign campaign, @amount amount) S
            ON T.month = S.month AND T.platform = S.platform AND T.campaign = S.campaign
            WHEN MATCHED AND S.amount IS NULL THEN DELETE
            WHEN MATCHED THEN UPDATE SET amount = S.amount, updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED AND S.amount IS NOT NULL THEN
              INSERT (month, platform, campaign, amount, updated_at)
              VALUES (S.month, S.platform, S.campaign, S.amount, CURRENT_TIMESTAMP())`,
    params: { month: p.month, platform: p.platform, campaign: p.campaign,
              amount: (p.amount == null || p.amount === '') ? null : Number(p.amount) },
    types: { month: 'STRING', platform: 'STRING', campaign: 'STRING', amount: 'FLOAT64' }
  })
};

let _client;
function client() {
  if (_client) return _client;
  if (!process.env.GCP_SA_KEY) throw new Error('Falta la variable de entorno GCP_SA_KEY');
  let creds;
  try { creds = JSON.parse(process.env.GCP_SA_KEY); }
  catch (e) { throw new Error('GCP_SA_KEY no es un JSON valido'); }
  _client = new BigQuery({ projectId: PROJECT, credentials: creds });
  return _client;
}

function readBody(req) {
  return new Promise(resolve => {
    if (req.body && typeof req.body === 'object') return resolve(req.body);
    let d = '';
    req.on('data', c => d += c);
    req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch (e) { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') { res.status(405).json({ error: 'Metodo no permitido' }); return; }
  if (process.env.APP_KEY && req.headers['x-app-key'] !== process.env.APP_KEY) {
    res.status(401).json({ error: 'No autorizado' }); return;
  }
  try {
    const body = await readBody(req);
    const builder = QUERIES[body.kind];
    if (!builder) { res.status(400).json({ error: 'kind invalido' }); return; }
    const { query, params, types } = builder(body.params || {});
    const opts = { query, params, location: 'US' };
    if (types) opts.types = types;
    const [rows] = await client().query(opts);
    res.status(200).json({ rows });
  } catch (e) {
    res.status(500).json({ error: (e && e.message) || 'Error de query' });
  }
};
